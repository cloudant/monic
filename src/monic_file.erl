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
-export([open/1, open_new/1, close/1, add/5, info/3, read/3]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
          data_start_pos,
          tid,
          index_fd=nil,
          main_fd=nil,
          next_index,
          reset_pos,
          sha,
          write_pos,
          writer=nil
         }).

-type streambody() :: fun(({binary(), fun() | done}) -> streambody()).

%% public functions

-spec open(list()) -> {ok, pid()} | {error, term()}.
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

-spec add(pid(), binary(), integer(), integer(), streambody()) -> ok | {error, term()}.
add(Pid, Key, Cookie, Size, StreamBody) ->
    case gen_server:call(Pid, {start_writing, Key, Cookie, Size}) of
        {ok, Ref} ->
            stream_in(Pid, Ref, StreamBody);
        Else ->
            Else
    end.

-spec info(pid(), binary(), integer()) -> {ok, {integer(), integer(), integer()}} | {error, term()}.
info(Pid, Key, Cookie) ->
    gen_server:call(Pid, {info, Key, Cookie}).

-spec read(pid(), binary(), integer()) -> ok | {error, term()}.
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
        {ok, IndexFd, LastLoc} ->
            case load_main(Tid, Path, LastLoc) of
                {ok, MainFd, Eof} ->
                    {ok, #state{
                       index_fd=IndexFd,
                       main_fd=MainFd,
                       reset_pos=Eof,
                       write_pos=Eof,
                       tid=Tid
                      }};
                Else ->
                    {stop, Else}
            end;
        Else ->
            {stop, Else}
    end.

handle_call({start_writing, Key, Cookie, Size}, _From,
            #state{main_fd=MainFd, write_pos=Pos, writer=nil}=State) ->
    Ref = make_ref(),
    LastModified = erlang:universaltime(),
    Header = #header{cookie=Cookie, key=Key, size=Size, last_modified=LastModified},
    Index = #index{cookie=Cookie, key=Key, location=Pos, size=Size, last_modified=LastModified},
    case monic_utils:pwrite_term(MainFd, Pos, Header) of
        {ok, HeaderSize} ->
            {reply, {ok, Ref}, State#state{data_start_pos=Pos + HeaderSize, sha=crypto:sha_init(),
                                           next_index=Index, write_pos=Pos + HeaderSize, writer=Ref}};
        Else ->
            {reply, Else, abandon_write(State)}
    end;
handle_call({start_writing, _Key, _Cookie, _Size}, _From, State) ->
    {reply, {error, already_writing}, State};

handle_call({write, Ref, {Bin, Next}}, _From, #state{main_fd=Fd, sha=Sha, write_pos=Pos, writer=Ref}=State) ->
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
    Sha1 = crypto:sha_update(Sha, Bin),
    case {Next, Write} of
        {done, ok} ->
            Footer = #footer{sha=crypto:sha_final(Sha1)},
            case monic_utils:pwrite_term(Fd, Pos + Size, Footer) of
                {ok, FooterSize} ->
                    case file:datasync(Fd) of
                        ok ->
                            {ok, IndexPos} = file:position(Fd, cur), %% TODO track this in state.
                            monic_utils:pwrite_term(State#state.index_fd, IndexPos, Index),
                            ets:insert(State#state.tid, {Index#index.key, Index#index.cookie,
                                Index#index.location, Index#index.size, Index#index.last_modified}),
                            {reply, ok, State#state{next_index=nil, reset_pos=Pos + Size + FooterSize,
                                write_pos=Pos + Size + FooterSize, writer=nil}};
                        Else ->
                            {reply, Else, abandon_write(State)}
                    end;
                Else ->
                    {reply, Else, abandon_write(State)}
            end;
        {_, ok} ->
            {reply, {continue, Next}, State#state{sha=Sha1, write_pos=Pos + Size}};
        {_, Else} ->
            {reply, Else, abandon_write(State)}
    end;
handle_call({write, _Ref, _StreamBody}, _From, State) ->
    {reply, {error, not_writing}, State};

handle_call({read, Key, Cookie}, _From, #state{main_fd=Fd, tid=Tid}=State) ->
    case info_int(Tid, Key, Cookie) of
        {ok, {Location, Size, _LastModified}} ->
            {ok, HeaderSize, _} = monic_utils:pread_term(Fd, Location),
            Self = self(),
            {reply, {ok, fun() -> stream_out(Self, Location + HeaderSize, Size) end}, State};
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
    case file:open(Path ++ ".idx", [binary, raw, read, write]) of
        {ok, Fd} ->
            case load_index_items(Tid, Fd) of
                {ok, LastLoc} ->
                    {ok, Fd, LastLoc};
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

load_index_items(Tid, Fd) ->
    load_index_items(Tid, Fd, 0, 0).

load_index_items(Tid, Fd, IndexLocation, LastLoc) ->
    case monic_utils:pread_term(Fd, IndexLocation) of
        {ok, IndexSize, #index{key=Key,cookie=Cookie,location=Location,size=Size,
                               last_modified=LastModified,deleted=Deleted}} ->
            case Deleted of
                false -> ets:insert(Tid, {Key, Cookie, Location, Size, LastModified});
                true -> ets:delete(Tid, Key)
            end,
            load_index_items(Tid, Fd, IndexLocation + IndexSize, Location);
        eof ->
            {ok, LastLoc};
        Else ->
            Else
    end.

load_main(Tid, Path, LastLoc) ->
    case file:open(Path, [binary, raw, read, write]) of
        {ok, Fd} ->
            case load_main_items(Tid, Fd, LastLoc) of
                {ok, Eof} ->
                    {ok, Fd, Eof};
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

load_main_items(Tid, Fd, Location) ->
    case monic_utils:pread_term(Fd, Location) of
        {ok, HeaderSize, #header{key=Key,cookie=Cookie,size=Size,last_modified=LastModified,deleted=Deleted}} ->
            case Deleted of
                false -> ets:insert(Tid, {Key, Cookie, Location, Size, LastModified});
                true -> ets:delete(Tid, Key)
            end,
            case monic_utils:pread_term(Fd, Location + HeaderSize + Size) of
                {ok, FooterSize, _} ->
                    load_main_items(Tid, Fd, Location + HeaderSize + Size + FooterSize);
                eof ->
                    {ok, Location}
            end;
        eof ->
            {ok, Location};
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
        {ok, Cookie} ->
            {ok, Cookie};
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
