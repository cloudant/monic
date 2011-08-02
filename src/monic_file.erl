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
-export([create/1, delete/1, open/1, open_new/1, close/1, compact/1]).
-export([add/5, delete/3, info/3, read/3, sync/1]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-define(INDEX_MODULE, monic_index_ets).

-record(state, {
          index,
          index_fd=nil,
          main_fd=nil,
          next_header,
          path,
          pending=queue:new(),
          remaining,
          reset_pos,
          sha,
          written,
          writer=nil,
          monitor=nil
         }).

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

compact(Pid) ->
    gen_server:call(Pid, compact, infinity).

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
        ok ->
            stream_in(Pid, StreamBody);
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

%% Sync the file (the main file is always synced, so this just syncs the index)
-spec sync(pid()) -> ok | {error, term()}.
sync(Pid) ->
    gen_server:call(Pid, sync, infinity).

%% gen_server functions

init(Path) ->
    case init_int(Path) of
        {ok, State} ->
            {ok, State};
        Else ->
            {stop, Else}
    end.

handle_call({start_writing, Key, Cookie, Size}, {Pid,_}, #state{writer=nil}=State) ->
    {Reply, State1} = start_write(Key, Cookie, Size, Pid, State),
    {reply, Reply, State1};
handle_call({start_writing, Key, Cookie, Size}, From, #state{pending=Pending}=State) ->
    {noreply, State#state{pending=queue:in({Key, Cookie, Size, From}, Pending)}};

handle_call({write, {Bin, Next}}, {Pid, _}, #state{main_fd=Fd, next_header=Header,
                                                   remaining=Remaining, written=Written,
                                                   reset_pos=Pos, sha=Sha, writer=Pid}=State) ->
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
                            monic_utils:write_term(State#state.index_fd, Header),
                            ?INDEX_MODULE:insert(State#state.index, Header),
                            {reply, ok, finish_write(Pos + Written + Size + FooterSize, State)};
                        Else ->
                            {reply, Else, abandon_write(State)}
                    end;
                Else ->
                    {reply, Else, abandon_write(State)}
            end;
        {_, ok} ->
            {reply, {continue, Next}, State#state{remaining=Remaining-Size,
                                                  written=Written+Size, sha=Sha1}};
        {_, Else} ->
            {reply, Else, abandon_write(State)}
    end;
handle_call({write, _StreamBody}, _From, State) ->
    {reply, {error, not_writing}, State};

handle_call({read, Key, Cookie}, _From, #state{main_fd=Fd, index=Index}=State) ->
    case info_int(Index, Key, Cookie) of
        {ok, #header{location=Location,size=Size}} ->
            {ok, HeaderSize, _} = monic_utils:pread_term(Fd, Location),
            Self = self(),
            {reply, {ok, fun() -> stream_out(Self, Location + HeaderSize, Size) end}, State};
        Else ->
            {reply, Else, State}
    end;
handle_call({read_hunk, Location, Size}, _From, #state{main_fd=Fd}=State) ->
    {reply, file:pread(Fd, Location, Size), State};

handle_call({delete, Key, Cookie}, _From, #state{index_fd=IndexFd,main_fd=MainFd,reset_pos=Pos,index=Index}=State) ->
    case info_int(Index, Key, Cookie) of
        {ok, _} ->
            case monic_utils:write_term(MainFd, #header{key=Key,deleted=true}) of
                {ok, HeaderSize} ->
                    case file:datasync(MainFd) of
                        ok ->
                            monic_utils:write_term(IndexFd, #header{key=Key,cookie=Cookie,deleted=true}),
                            ?INDEX_MODULE:delete(Index, Key),
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

handle_call({info, Key, Cookie}, _From, #state{index=Index}=State) ->
    {reply, info_int(Index, Key, Cookie), State};

handle_call(close, _From, State) ->
    {stop, normal, ok, cleanup(State)};

handle_call(compact, _From, #state{path=Path}=State) ->
    case compact_int(State) of
        ok ->
            case init_int(Path) of
                {ok, State1} ->
                    cleanup(State),
                    {reply, ok, State1};
                Else ->
                    {reply, Else, State}
            end;
        Else ->
            {reply, Else, State}
    end;
handle_call(sync, _From, #state{index_fd=Fd}=State) ->
    {reply, file:sync(Fd), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Mon, _, _, _}, #state{monitor=Mon}=State) ->
    {noreply, abandon_write(State)}.

terminate(_Reason, State) ->
    cleanup(State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% private functions

load_index(Index, Path) ->
    case file:open(Path ++ ".idx", [binary, raw, read, append]) of
        {ok, Fd} ->
            case load_index_items(Index, Fd) of
                {ok, LastLoc} ->
                    {ok, Fd, LastLoc};
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

load_index_items(Index, Fd) ->
    load_index_items(Index, Fd, 0, 0).

load_index_items(Index, Fd, IndexLocation, Eof) ->
    case monic_utils:pread_term(Fd, IndexLocation) of
        {ok, IndexSize, #header{deleted=true,key=Key}} ->
            ?INDEX_MODULE:delete(Index, Key),
            load_index_items(Index, Fd, IndexLocation + IndexSize, Eof);
        {ok, IndexSize, #header{deleted=false,location=Location}=Header} ->
            ?INDEX_MODULE:insert(Index, Header),
            load_index_items(Index, Fd, IndexLocation + IndexSize, Location);
        eof ->
            {ok, Eof};
        Else ->
            Else
    end.

load_main(Index, Path, IndexFd, Eof) ->
    case file:open(Path, [binary, raw, read, append]) of
        {ok, MainFd} ->
            case load_main_items(Index, MainFd, IndexFd, Eof) of
                {ok, Eof1} ->
                    {ok, MainFd, Eof1};
                Else ->
                    Else
            end;
        Else ->
            Else
    end.

load_main_items(Index, MainFd, IndexFd, Location) ->
    case monic_utils:pread_term(MainFd, Location) of
        {ok, HeaderSize, #header{deleted=true,key=Key}=Header} ->
            ?INDEX_MODULE:delete(Index, Key),
            monic_utils:write_term(IndexFd, Header),
            load_main_items(Index, MainFd, IndexFd, Location + HeaderSize);
        {ok, HeaderSize, #header{deleted=false,size=Size}=Header} ->
            Header1 = Header#header{location=Location},
            ?INDEX_MODULE:insert(Index, Header1),
            monic_utils:write_term(IndexFd, Header1),
            case monic_utils:pread_term(MainFd, Location + HeaderSize + Size) of
                {ok, FooterSize, _} ->
                    load_main_items(Index, MainFd, IndexFd, Location + HeaderSize + Size + FooterSize);
                eof ->
                    truncate(MainFd, Location),
                    {ok, Location}
            end;
        eof ->
            truncate(MainFd, Location),
            {ok, Location};
        Else ->
            Else
    end.

start_write(Key, Cookie, Size, WriterPid,
            #state{main_fd=MainFd,reset_pos=Pos,writer=nil}=State) ->
    LastModified = erlang:universaltime(),
    Header = #header{
      cookie=Cookie,
      key=Key,
      location=Pos,
      size=Size,
      last_modified=LastModified
     },
    case monic_utils:write_term(MainFd, Header#header{location=nil}) of
        {ok, HeaderSize} ->
            {ok, State#state{
                   remaining=Size,
                   sha=crypto:sha_init(),
                   next_header=Header,
                   writer=WriterPid,
                   monitor=monitor(process, WriterPid),
                   written=HeaderSize}};
        Else ->
            {Else, abandon_write(State)}
    end.

finish_write(Eof, State) ->
    maybe_start_pending_write(
      clear_writer(State#state{next_header=nil,
                               reset_pos=Eof})).

abandon_write(#state{main_fd=Fd, reset_pos=Pos}=State) ->
    truncate(Fd, Pos),
    maybe_start_pending_write(clear_writer(State)).

clear_writer(State) ->
    demonitor(State#state.monitor),
    State#state{writer=nil,monitor=nil}.

maybe_start_pending_write(#state{pending=Pending}=State) ->
    case queue:out(Pending) of
        {empty, Pending1} ->
            State#state{pending=Pending1};
        {{value, {Key, Cookie, Size, {Pid,_}=From}}, Pending1} ->
            {Reply, State1} = start_write(Key, Cookie, Size, Pid, State#state{pending=Pending1}),
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

stream_in(Pid, StreamBody) ->
    case gen_server:call(Pid, {write, StreamBody}, infinity) of
        {ok, Cookie} ->
            {ok, Cookie};
        {continue, Next} ->
            stream_in(Pid, Next());
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

info_int(Index, Key, Cookie) ->
    case ?INDEX_MODULE:lookup(Index, Key) of
        {ok, #header{cookie=Cookie}=Header} ->
            {ok, Header};
        Else ->
            Else
    end.

cleanup(#state{index=Index,index_fd=IndexFd,main_fd=MainFd}=State) ->
    close_int(IndexFd),
    close_int(MainFd),
    close_index(Index),
    State#state{index=nil,index_fd=nil,main_fd=nil}.

close_int(nil) ->
    ok;
close_int(Fd) ->
    file:close(Fd).

close_index(nil) ->
    ok;
close_index(Index) ->
    ?INDEX_MODULE:stop(Index).

init_int(Path) ->
    {ok, Index} = ?INDEX_MODULE:start_link(),
    case load_index(Index, Path) of
        {ok, IndexFd, LastLoc} ->
            {ok, MainFd, Eof} = load_main(Index, Path, IndexFd, LastLoc),
            {ok, #state{
               index_fd=IndexFd,
               main_fd=MainFd,
               path=Path,
               reset_pos=Eof,
               index=Index
              }};
        Else ->
            Else
    end.

compact_int(#state{index_fd=IndexFd, main_fd=MainFd, path=Path, index=Index}) ->
    {ok, CompactFd} = file:open(Path ++ ".compact", [binary, raw, append]),
    ok = compact_int(IndexFd, MainFd, CompactFd, Index, 0),
    ok = file:datasync(CompactFd),
    file:delete(Path ++ ".idx"), %% TODO file:rename(Path ++ ".compact.idx", Path ++ ".idx")
    file:rename(Path ++ ".compact", Path).

compact_int(IndexFd, MainFd, CompactFd, Index, IndexLocation) ->
    case monic_utils:pread_term(IndexFd, IndexLocation) of
        {ok, IndexHeaderSize, #header{key=Key,location=OldLocation,size=Size}=Header} ->
            case ?INDEX_MODULE:lookup(Index, Key) of
                {ok, _} ->
                    {ok, CompactHeaderSize} = monic_utils:write_term(CompactFd, Header#header{location=nil}),
                    ok = copy_item(MainFd, CompactFd, OldLocation + CompactHeaderSize, Size),
                    {ok, _, #footer{}=Footer} =  monic_utils:pread_term(MainFd, OldLocation + CompactHeaderSize + Size),
                    {ok, _} = monic_utils:write_term(CompactFd, Footer),
                    compact_int(IndexFd, MainFd, CompactFd, Index,
                                IndexLocation + IndexHeaderSize);
                _ ->
                    compact_int(IndexFd, MainFd, CompactFd, Index, IndexLocation + IndexHeaderSize)
            end;
        eof ->
            ok
    end.

copy_item(_FromFd, _ToFd, _Location, 0) ->
    ok;
copy_item(FromFd, ToFd, Location, Remaining) when Remaining > 0 ->
    {ok, Bin} = file:pread(FromFd, Location, min(Remaining, ?BUFFER_SIZE)),
    Size = iolist_size(Bin),
    ok = file:write(ToFd, Bin),
    copy_item(FromFd, ToFd, Location + Size, Remaining - Size).
