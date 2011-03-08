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
    next_location=0,
    index_fd=nil,
    main_fd=nil,
    next_key=0,
    tid=nil
}).

-define(BUFFER_SIZE, 16384).

%% public API
-export([open/1, write/3, close/1]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

%% public functions

open(Path) ->
    gen_server:start_link({local, list_to_atom(Path)}, ?MODULE, Path, []).

write(Pid, Size, Fun) ->
    gen_server:call(Pid, {write, Size, Fun}, infinity).

close(Pid) ->
    gen_server:call(Pid, close, infinity).

%% gen_server functions

init(Path) ->
    Tid = ets:new(index, [set, private]),
    case load_index(Tid, Path) of
        {ok, IndexFd, Hints} ->
            case load_main(Tid, Path, Hints) of
                {ok, MainFd, {NextKey, NextLocation}} ->
                    {ok, #state{
                        index_fd=IndexFd,
                        main_fd=MainFd,
                        next_key=NextKey,
                        next_location=NextLocation,
                        tid=Tid
                    }};
                Else ->
                    {stop, Else}
            end;
        Else ->
            {stop, Else}
    end.

handle_call({write, Size, Fun}, _From, #state{main_fd=Fd,next_location=NextLocation}=State) ->
    case new_item(Size, Fun, State) of
        {ok, Key, Cookie} ->
            {reply, {ok, Key, Cookie}, State#state{
                next_key = Key + 1,
                next_location = NextLocation + Size + ?HEADER_SIZE + ?FOOTER_SIZE
            }};
        Else ->
            file:position(Fd, NextLocation),
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
            case load_index_items(Tid, Fd) of
                {ok, Hints} ->
                    {ok, Fd, Hints};
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

load_index_items(Tid, Fd) ->
    load_index_items(Tid, Fd, {0, 0}).

load_index_items(Tid, Fd, Hints) ->
    case monic_utils:read_index(Fd) of
        {ok, #index{key=Key,location=Location,size=Size,version=Version,flags= <<Deleted:1,_:15>>}} ->
            case Deleted of
                0 -> ets:insert(Tid, {Key, Location, Size, Version});
                1 -> ets:delete(Tid, Key)
            end,
            load_index_items(Tid, Fd, {Key + 1, Location + Size + ?HEADER_SIZE + ?FOOTER_SIZE});
        eof ->
            {ok, Hints};
        Else ->
            Else
    end.

load_main(Tid, Path, Hints) ->
    case file:open(Path, [binary, raw, read, write]) of
        {ok, Fd} ->
            case load_main_items(Tid, Fd, Hints) of
                {ok, Hints} ->
                    {ok, Fd, Hints};
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

load_main_items(Tid, Fd, {_, Location}=Hints) ->
    case monic_utils:pread_header(Fd, Location) of
        {ok, #header{key=Key,cookie=_Cookie,size=Size,version=Version,flags= <<Deleted:1,_:15>>}} ->
            case Deleted of
                0 -> ets:insert(Tid, {Key, Location, Size, Version});
                1 -> ets:delete(Tid, Key)
            end,
            load_main_items(Tid, Fd, {Key + 1, Location + Size + ?HEADER_SIZE + ?FOOTER_SIZE});
        eof ->
            {ok, Hints};
        Else ->
            Else
    end.

new_item(Size, Fun, #state{tid=Tid, index_fd=IndexFd, main_fd=MainFd,
    next_key=Key, next_location=Location}) ->
    Cookie = monic_utils:new_cookie(),
    Version = 1,
    Header = #header{key=Key, cookie=Cookie, size=Size, version=Version, flags=0},
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
                                    {ok, Key, Cookie};
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

