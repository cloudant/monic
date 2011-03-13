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

-module(monic_file).
-behavior(gen_server).
-include("monic.hrl").

%% public API
-export([open/1, open_new/1, close/1, add/3, info/3, read/3]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
    data_start_pos,
    tid,
    index_fd=nil,
    main_fd=nil,    
    next_index,
    next_key,
    reset_pos,
    write_pos,
    writer=nil
}).

%% public functions

open(Path) ->
    case open_new(Path) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        Else ->
            Else
    end.

open_new(Path) ->
    gen_server:start_link({local, list_to_atom(Path)}, ?MODULE, Path, []).

close(Pid) ->
    gen_server:call(Pid, close, infinity).

add(Pid, Size, StreamBody) ->
    case gen_server:call(Pid, {start_writing, Size}) of
        {ok, Ref} ->
            stream_in(Pid, Ref, StreamBody);
        Else ->
            Else
    end.

info(Pid, Key, Cookie) ->
    gen_server:call(Pid, {info, Key, Cookie}).

read(Pid, Key, Cookie) ->
    case gen_server:call(Pid, {read, Key, Cookie}) of
        {ok, StreamBodyFun} ->
            {ok, StreamBodyFun()};
        Else ->
            Else
    end.

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
                        reset_pos=NextLocation,
                        write_pos=NextLocation,                        
                        tid=Tid
                    }};
                Else ->
                    {stop, Else}
            end;
        Else ->
            {stop, Else}
    end.

handle_call({start_writing, Size}, _From,
    #state{main_fd=MainFd, next_key=Key, write_pos=Pos, writer=nil}=State) ->
    Ref = make_ref(),
    Cookie = monic_utils:new_cookie(),
    Flags = <<0:16>>,
    Version = 1,
    Header = #header{cookie=Cookie, flags=Flags, key=Key, size=Size, version=Version},
    Index = #index{cookie=Cookie, key=Key, flags=Flags, location=Pos, size=Size, version=Version},
    case monic_utils:pwrite_header(MainFd, Pos, Header) of
        ok ->
            {reply, {ok, Ref}, State#state{data_start_pos=Pos+?HEADER_SIZE,
                next_index=Index, write_pos=Pos + ?HEADER_SIZE, writer=Ref}};
        Else ->
            {reply, Else, abandon_write(State)}
    end;
handle_call({start_writing, _Size}, _From, State) ->
    {reply, {error, already_writing}, State};

handle_call({write, Ref, {Bin, Next}}, _From, #state{main_fd=Fd, write_pos=Pos, writer=Ref}=State) ->
    Index = State#state.next_index,
    Size = iolist_size(Bin),
    Remaining = Index#index.size - (Pos + Size - State#state.data_start_pos),
    Write = case {Next, Remaining} of
        {_, Remaining} when Remaining < 0 ->
            {error, overflow};
        {done, Remaining} when Remaining > 0 ->
            {error, underflow};
        _ ->
            file:pwrite(Fd, Pos, Bin)
    end,
    case {Next, Write} of
        {done, ok} ->
            case file:datasync(Fd) of
                ok ->
                    monic_utils:write_index(State#state.index_fd, Index),
                    ets:insert(State#state.tid, {Index#index.key, Index#index.cookie,
                        Index#index.location, Index#index.size, Index#index.version}),
                    {reply, {ok, {Index#index.key, Index#index.cookie}},
                        State#state{next_index=nil, next_key=State#state.next_key+1,
                        reset_pos=Pos + Size, write_pos=Pos + Size, writer=nil}};
                Else ->
                    {reply, Else, abandon_write(State)}
                end;
        {_, ok} ->
            {reply, {continue, Next}, State#state{write_pos=Pos + Size}};
        {_, Else} ->
            {reply, Else, abandon_write(State)}
    end;
handle_call({write, _Ref, _StreamBody}, _From, State) ->
    {reply, {error, not_writing}, State};

handle_call({read, Key, Cookie}, _From, #state{tid=Tid}=State) ->
    case info_int(Tid, Key, Cookie) of
        {ok, {Location, Size, _Version}} ->
            Self = self(),
            {reply, {ok, fun() -> stream_out(Self, Location + ?HEADER_SIZE, Size) end}, State};
        Else ->
            {reply, Else, State}
    end;
handle_call({read_hunk, Location, Size}, _From, #state{main_fd=Fd}=State) ->
    {reply, file:pread(Fd, Location, Size), State};

handle_call({info, Key, Cookie}, _From, #state{tid=Tid}=State) ->
    {reply, info_int(Tid, Key, Cookie), State};

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
        {ok, #index{key=Key,cookie=Cookie,location=Location,size=Size,version=Version,flags= <<Deleted:1,_:15>>}} ->
            case Deleted of
                0 -> ets:insert(Tid, {Key, Cookie, Location, Size, Version});
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
        {ok, #header{key=Key,cookie=Cookie,size=Size,version=Version,flags= <<Deleted:1,_:15>>}} ->
            case Deleted of
                0 -> ets:insert(Tid, {Key, Cookie, Location, Size, Version});
                1 -> ets:delete(Tid, Key)
            end,
            load_main_items(Tid, Fd, {Key + 1, Location + Size + ?HEADER_SIZE + ?FOOTER_SIZE});
        eof ->
            {ok, Hints};
        Else ->
            Else
    end.

abandon_write(#state{main_fd=Fd, reset_pos=Pos}=State) ->
    case file:position(Fd, Pos) of
        {ok, Pos} ->
            file:truncate(Fd);
        _ ->
            ok
    end,
    State#state{write_pos=Pos, writer=nil}.

stream_in(Pid, Ref, StreamBody) ->
    case gen_server:call(Pid, {write, Ref, StreamBody}, infinity) of
        {ok, Key, Cookie} ->
            {ok, Key, Cookie};
        {continue, Next} ->
            stream_in(Pid, Ref, Next());
        Else ->
            Else
    end.

stream_out(Pid, Location, Remaining) ->
    case gen_server:call(Pid, {read_hunk, Location, min(Remaining, ?BUFFER_SIZE)}) of
        {ok, Bin} ->
            Size = iolist_size(Bin),
            case Remaining - Size of
                0 ->
                    {Bin, done};
                _ ->
                    {Bin, fun() -> stream_out(Pid, Location + Size, Remaining - Size) end}
            end;
        _ ->
            %% better way to terminate?
            {<<>>, done}
    end.

info_int(Tid, Key, Cookie) ->
   case ets:lookup(Tid, Key) of
        [{Key, Cookie, Location, Size, Version}] ->
            {ok, {Location, Size, Version}};
        _ ->
            {error, not_found}
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

