%% Copyright 2011 Cloudant
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(monic_file_writer).
-behavior(gen_server).
-include("monic.hrl").

-record(state, {
    tid=nil,
    index_fd=nil,
    main_fd=nil,
    eof
}).

-define(BUFFER_SIZE, 16384).

%% public API
-export([open/1, write/5, close/1]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

%% public functions

open(Path) ->
    gen_server:start_link({local, list_to_atom(Path)}, ?MODULE, Path, []).

write(Pid, Key, Cookie, Size, Fun) ->
    gen_server:call(Pid, {write, Key, Cookie, Size, Fun}, infinity).

close(Pid) ->
    gen_server:call(Pid, close, infinity).

%% gen_server functions

init(Path) ->
    Tid = ets:new(index, [set, private]),
    case load_index(Tid, Path) of
        {ok, IndexFd, Hint} ->
            case load_main(Tid, Path, Hint) of
                {ok, MainFd, Eof} ->
                    {ok, #state{tid=Tid,index_fd=IndexFd,main_fd=MainFd,eof=Eof}};
                Else ->
                    {stop, Else}
            end;
        Else ->
            {stop, Else}
    end.

handle_call({write, Key, Cookie, Size, Fun}, _From, #state{main_fd=Fd,eof=Eof}=State) ->
    case update_item(Key, Cookie, Size, Fun, State) of
        {ok, Eof1} ->
            {reply, ok, State#state{eof=Eof1}};
        Else ->
            file:position(Fd, Eof),
            file:truncate(Fd),
            {reply, Else, State}
    end;
handle_call(close, _From, State) ->
    {stop, normal, ok, cleanup(State)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    cleanup(State).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% private functions

load_index(Tid, Path) ->
    case file:open(Path ++ ".idx", [binary, raw, read, write, append]) of
        {ok, Fd} ->
            case load_index_items(Tid, Fd, 0) of
                {ok, Hint} ->
                    {ok, Fd, Hint};
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

load_index_items(Tid, Fd, Hint) ->
    case monic_utils:read_index(Fd) of
        {ok, #index{key=Key,location=Location,size=Size,version=Version,flags= <<Deleted:1,_:15>>}} ->
            case Deleted of
                0 -> ets:insert(Tid, {Key, Location, Size, Version});
                1 -> ets:delete(Tid, Key)
            end,
            load_index_items(Tid, Fd, Location);
        eof ->
            {ok, Hint};
        Else ->
            Else
    end.

load_main(Tid, Path, Hint) ->
    case file:open(Path, [binary, raw, read, write]) of
        {ok, Fd} ->
            case load_main_items(Tid, Fd, Hint) of
                {ok, Eof} ->
                    {ok, Fd, Eof};
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

load_main_items(Tid, Fd, Location) ->
    case monic_utils:pread_header(Fd, Location) of
        {ok, #header{key=Key,cookie=_Cookie,size=Size,version=Version,flags= <<Deleted:1,_:15>>}} ->
            case Deleted of
                0 -> ets:insert(Tid, {Key, Location, Size, Version});
                1 -> ets:delete(Tid, Key)
            end,
            load_main_items(Tid, Fd, Location + Size + ?HEADER_SIZE + ?FOOTER_SIZE);
        eof ->
            {ok, Location};
        Else ->
            Else
    end.

update_item(Key, Cookie, Size, Fun, #state{tid=Tid,index_fd=IndexFd,main_fd=MainFd,eof=Location}) ->
    Version = case ets:lookup(Tid, Key) of
        [] -> 1;
        [65535] -> 1;
        [V] -> V + 1
    end,
    Header = #header{key=Key,cookie=Cookie,size=Size,version=Version, flags=0},
    case monic_utils:pwrite_header(MainFd, Location, Header) of
        ok ->
            case copy_in(MainFd, Fun, Location + ?HEADER_SIZE, Size) of
                {ok, Sha} ->
                    Footer = #footer{sha=Sha},
                    case monic_utils:pwrite_footer(MainFd, Location + ?HEADER_SIZE + Size, Footer) of
                        ok ->
                            case file:datasync(MainFd) of
                                ok ->
                                    monic_utils:write_index(IndexFd,
                                    #index{key=Key,location=Location,size=Size,version=Version,flags=0}
                                    ),
                                    ets:insert(Tid, {Key, Location, Size, Version}),
                                    {ok, Location + ?HEADER_SIZE + Size + ?FOOTER_SIZE};
                                Else ->
                                    Else
                            end;
                        Else ->
                            Else
                    end;
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

copy_in(Fd, Fun, Location, Remaining) ->
    copy_in(Fd, Fun, Location, Remaining, crypto:sha_init()).

copy_in(_Fd, _Fun, _Location, 0, Sha) ->
    {ok, crypto:sha_final(Sha)};
copy_in(Fd, Fun, Location, Remaining, Sha) ->
    case Fun(?BUFFER_SIZE) of
        {ok, Bin} ->
            Size = iolist_size(Bin),
            case Size =< Remaining of
                true ->                    
                    case file:pwrite(Fd, Location, Bin) of
                        ok ->
                            copy_in(Fd, Fun, Location + Size,
                                Remaining - Size,
                                crypto:sha_update(Sha, Bin));
                        Else ->
                            Else
                    end;
                false ->
                    {error, overflow}
            end;
        Else ->
            Else
    end.

cleanup(#state{tid=Tid,index_fd=IndexFd,main_fd=MainFd}=State) ->
    close_int(IndexFd),
    close_int(MainFd),
    close_ets(Tid),
    State#state{tid=nil,index_fd=nil,main_fd=nil}.
    
close_int(nil) ->
    ok;
close_int(Fd) ->
    file:close(Fd).

close_ets(nil) ->
    ok;
close_ets(Tid) ->
    ets:delete(Tid).

