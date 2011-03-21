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
-export([create/1, delete/1, open/1, open_new/1, close/1, add/5, delete/3, info/3, read/3]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
          tid,
          header_size,
          index_fd=nil,
          last_write=nil,
          main_fd=nil,
          next_index,
          pending=queue:new(),
          remaining,
          reset_pos,
          sha,
          writer=nil
         }).

-define(IDLE_WRITER_TIMEOUT, 5000).
-type streambody() :: {binary(), done | fun(() -> streambody())}.

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

create(Path) ->
    file:write_file(Path, <<>>, [exclusive]) == ok.

delete(Path) ->
    case file:delete(Path ++ ".idx") == ok andalso file:delete(Path) == ok of
        true ->
            case open(Path) of
                {ok, Pid} ->
                    close(Pid); %% ensure fds are released.
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

-spec add(pid(), binary(), integer(), integer(), streambody()) -> ok | {error, term()}.
add(Pid, Key, Cookie, Size, StreamBody) ->
    case gen_server:call(Pid, {start_writing, Key, Cookie, Size}, infinity) of
        {ok, Ref} ->
            stream_in(Pid, Ref, StreamBody);
        Else ->
            Else
    end.

-spec delete(pid(), binary(), integer()) -> ok | {error, term()}.
delete(Pid, Key, Cookie) ->
    gen_server:call(Pid, {delete, Key, Cookie}, infinity).

-spec info(pid(), binary(), integer()) -> {ok, {integer(), integer(), integer()}} | {error, term()}.
info(Pid, Key, Cookie) ->
    gen_server:call(Pid, {info, Key, Cookie}).

-spec read(pid(), binary(), integer()) -> {ok, streambody()} | {error, term()}.
read(Pid, Key, Cookie) ->
    case gen_server:call(Pid, {read, Key, Cookie}) of
        {ok, StreamBodyFun} ->
            {ok, StreamBodyFun()};
        Else ->
            Else
    end.

%% gen_server functions

init(Path) ->
    Tid = ets:new(index, [{keypos, 2}, set, private]),
    case load_index(Tid, Path) of
        {ok, IndexFd, LastLoc} ->
            case load_main(Tid, Path, LastLoc) of
                {ok, MainFd, Eof} ->
                    {ok, #state{
                       index_fd=IndexFd,
                       main_fd=MainFd,
                       reset_pos=Eof,
                       tid=Tid
                      }};
                Else ->
                    {stop, Else}
            end;
        Else ->
            {stop, Else}
    end.

handle_call({start_writing, Key, Cookie, Size}, _From, #state{writer=nil}=State) ->
    {Reply, State1} = start_write(Key, Cookie, Size, State),
    {reply, Reply, State1};
handle_call({start_writing, Key, Cookie, Size}, From, #state{last_write=LastWrite, pending=Pending}=State) ->
    State1 = State#state{pending=queue:in({Key, Cookie, Size, From}, Pending)},
    case timer:now_diff(now(), LastWrite) > ?IDLE_WRITER_TIMEOUT of
        true ->
            erlang:display(recovering_from_writer_timeout),
            {noreply, abandon_write(State1)};
        false ->
            {noreply, State1}
    end;

handle_call({write, Ref, {Bin, Next}}, _From, #state{header_size=HeaderSize, main_fd=Fd, next_index=Index,
                                                     remaining=Remaining, reset_pos=Pos, sha=Sha, writer=Ref}=State) ->
    Size = iolist_size(Bin),
    Write = case {Next, Remaining - Size} of
                {_, Remaining1} when Remaining1 < 0 ->
                    {error, overflow};
                {done, Remaining1} when Remaining1 > 0 ->
                    {error, underflow};
                _ ->
                    file:write(Fd, Bin)
            end,
    Sha1 = crypto:sha_update(Sha, Bin),
    case {Next, Write} of
        {done, ok} ->
            Footer = #footer{sha=crypto:sha_final(Sha1)},
            case monic_utils:write_term(Fd, Footer) of
                {ok, FooterSize} ->
                    case file:datasync(Fd) of
                        ok ->
                            monic_utils:write_term(State#state.index_fd, Index),
                            ets:insert(State#state.tid, Index),
                            {reply, ok, finish_write(Pos + HeaderSize + Size + FooterSize, State)};
                        Else ->
                            {reply, Else, abandon_write(State)}
                    end;
                Else ->
                    {reply, Else, abandon_write(State)}
            end;
        {_, ok} ->
            {reply, {continue, Next}, State#state{last_write=now(), remaining=Remaining-Size, sha=Sha1}};
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

handle_call({delete, Key, Cookie}, _From, #state{index_fd=IndexFd,main_fd=MainFd,reset_pos=Pos,tid=Tid}=State) ->
    case info_int(Tid, Key, Cookie) of
        {ok, _} ->
            case monic_utils:write_term(MainFd, #header{key=Key,deleted=true}) of
                {ok, HeaderSize} ->
                    case file:datasync(MainFd) of
                        ok ->
                            monic_utils:write_term(IndexFd, #index{key=Key,cookie=Cookie,deleted=true}),
                            true = ets:delete(Tid, Key),
                            {reply, ok, State#state{reset_pos=Pos+HeaderSize}};
                        Else ->
                            {reply, Else, abandon_write(State)}
                    end;
                Else ->
                    {reply, Else, abandon_write(State)}
            end;
        Else ->
            {reply, Else, abandon_write(State)}
    end;

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
    case file:open(Path ++ ".idx", [binary, raw, read, append]) of
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

load_index_items(Tid, Fd, IndexLocation, Eof) ->
    case monic_utils:pread_term(Fd, IndexLocation) of
        {ok, IndexSize, #index{deleted=Deleted,key=Key,location=Location}=Index} ->
            case Deleted of
                false -> ets:insert(Tid, Index);
                true -> ets:delete(Tid, Key)
            end,
            load_index_items(Tid, Fd, IndexLocation + IndexSize, Location);
        eof ->
            {ok, Eof};
        Else ->
            Else
    end.

load_main(Tid, Path, Eof) ->
    case file:open(Path, [binary, raw, read, append]) of
        {ok, Fd} ->
            case load_main_items(Tid, Fd, Eof) of
                {ok, Eof1} ->
                    {ok, Fd, Eof1};
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

load_main_items(Tid, Fd, Location) ->
    case monic_utils:pread_term(Fd, Location) of
        {ok, HeaderSize, #header{deleted=Deleted,key=Key,size=Size}=Header} ->
            case Deleted of
                false ->
                    ets:insert(Tid, monic_utils:header_to_index(Header, Location)),
                    case monic_utils:pread_term(Fd, Location + HeaderSize + Size) of
                        {ok, FooterSize, _} ->
                            load_main_items(Tid, Fd, Location + HeaderSize + Size + FooterSize);
                        eof ->
                            truncate(Fd, Location),
                            {ok, Location}
                    end;
                true ->
                    ets:delete(Tid, Key),
                    load_main_items(Tid, Fd, Location + HeaderSize)
            end;
        eof ->
            truncate(Fd, Location),
            {ok, Location};
        Else ->
            Else
    end.

start_write(Key, Cookie, Size, #state{main_fd=MainFd,reset_pos=Pos,writer=nil}=State) ->
    Ref = make_ref(),
    LastModified = erlang:universaltime(),
    Header = #header{cookie=Cookie, key=Key, size=Size, last_modified=LastModified},
    Index = #index{cookie=Cookie, key=Key, location=Pos, size=Size, last_modified=LastModified},
    case monic_utils:write_term(MainFd, Header) of
        {ok, HeaderSize} ->
            {{ok, Ref}, State#state{
                          last_write=now(),
                          header_size=HeaderSize,
                          remaining=Size,
                          sha=crypto:sha_init(),
                          next_index=Index,
                          writer=Ref}};
        Else ->
            {Else, abandon_write(State)}
    end.

finish_write(Eof, State) ->
    maybe_start_pending_write(State#state{last_write=nil, next_index=nil, reset_pos=Eof, writer=nil}).

abandon_write(#state{main_fd=Fd, reset_pos=Pos}=State) ->
    truncate(Fd, Pos),
    maybe_start_pending_write(State#state{writer=nil}).

maybe_start_pending_write(#state{pending=Pending}=State) ->
    case queue:out(Pending) of
        {empty, Pending1} ->
            State#state{pending=Pending1};
        {{value, {Key, Cookie, Size, From}}, Pending1} ->
            {Reply, State1} = start_write(Key, Cookie, Size, State#state{pending=Pending1}),
            gen_server:reply(From, Reply),
            State1
    end.

truncate(Fd, Pos) ->
    case file:position(Fd, Pos) of
        {ok, Pos} ->
            file:position(Fd, Pos);
        Else ->
            Else
    end.

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
        [#index{cookie=Cookie,location=Location,size=Size,last_modified=LastModified}] ->
            {ok, {Location, Size, LastModified}};
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
