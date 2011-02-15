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
-include("monic.hrl").

%% public API
-export([start_link/1, close/1, write/2, read/2]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {path, fd, eof}).

-record(file_header, {uuid, eof}).
-define(FILE_HEADER_VERSION, 1).
-define(FILE_HEADER_SIZE, 4096).
-define(FILE_HEADER_MAGIC, 16067190828064835550).

-record(item_header, {cookie, flags=0, len}).
-define(ITEM_HEADER_VERSION, 1).
-define(ITEM_HEADER_SIZE, 36).
-define(ITEM_HEADER_MAGIC, 2998602972810243711).

-record(item_footer, {checksum}).
-define(ITEM_FOOTER_SIZE, 28).
-define(ITEM_FOOTER_MAGIC, 15866124023828541222).

%% public functions

start_link(Path) ->
    gen_server:start_link({local, list_to_atom(Path)}, ?MODULE, Path, []).

close(Pid) ->
    gen_server:call(Pid, close, infinity).

write(Pid, Bin) when is_binary(Bin) ->
    gen_server:call(Pid, {write, Bin}, infinity).

read(Pid, #handle{}=Handle) ->
    gen_server:call(Pid, {read, Handle}, infinity).

%% gen_server functions

init(Path) ->
    case file:open(Path, [read, write, raw, binary]) of
        {ok, Fd} ->
            case get_file_header(Fd) of
                {ok, #file_header{eof=Eof,uuid=UUID}} ->
                    {ok, #state{path=Path,eof=Eof,fd=Fd}};
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({write, Bin}, _From, #state{eof=Eof,fd=Fd}=State) ->
    Cookie = crypto:rand_bytes(16),
    ItemHeader = #item_header{cookie=Cookie, len=iolist_size(Bin)},
    Bin1 = [item_header_to_binary(ItemHeader), Bin],
    Size1 = iolist_size(Bin1),
    case file:pwrite(Fd, Eof, Bin1) of
        ok ->
            case file:datasync(Fd) of
                ok ->
                    Handle = #handle{location=Eof, cookie=Cookie},
                    {reply, {ok, Handle}, State#state{eof=Eof+Size1}};
                Else ->
                    {reply, Else, State}
            end;
        Else ->
            {reply, Else, State}
    end;
handle_call({read, #handle{location=Location,cookie=Cookie}=Handle}, _From, #state{fd=Fd}=State) ->
    case read_item_header(Fd, Location) of
        {ok, #item_header{cookie=Cookie,len=Len}} ->
            {reply, file:pread(Fd, Location+?ITEM_HEADER_SIZE, Len), State};
        {ok, #item_header{cookie=Cookie1}} ->
            {reply, {error, invalid_cookie}, State};
        Else ->
            Else
    end;
handle_call(close, _From, #state{fd=Fd}=State) ->
    {stop, normal, file:close(Fd), State#state{fd=nil}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{fd=nil}) ->
    ok;
terminate(Reason, #state{fd=Fd}) ->
    file:close(Fd).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

get_file_header(Fd) ->
    case read_file_header(Fd) of
        {ok, Header} ->
            {ok, Header};
        eof ->
            case write_file_header(Fd) of
                {ok, Header} ->
                    {ok, Header};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

read_file_header(Fd) ->
    case file:pread(Fd, 0, ?FILE_HEADER_SIZE) of
        {ok, Bin} ->
            binary_to_file_header(Bin);
        Error ->
            Error
    end.

write_file_header(Fd) ->
    NewHeader = #file_header{uuid=crypto:rand_bytes(16),eof=?FILE_HEADER_SIZE},
    write_file_header(Fd, NewHeader).

write_file_header(Fd, Header) ->
    file:pwrite(Fd, 0, file_header_to_iolist(Header)).

binary_to_file_header(Bin) ->
    case Bin of
        <<?FILE_HEADER_MAGIC:64/integer, ?FILE_HEADER_VERSION:16/integer,
          UUID:16/binary, Eof:64/integer, _Padding/binary>> ->
            {ok, #file_header{uuid=UUID,eof=Eof}};
        _ ->
            {error, invalid_file_header}
    end.

file_header_to_iolist(#file_header{uuid=UUID, eof=Eof}) ->
    Bin = <<?FILE_HEADER_MAGIC:64/integer, ?FILE_HEADER_VERSION:16/integer,
            UUID:16/binary, Eof:64/integer>>,
    Res = [Bin, <<0:(8*(?FILE_HEADER_SIZE - iolist_size(Bin)))>>].

read_item_header(Fd, Location) ->
    case file:pread(Fd, Location, ?ITEM_HEADER_SIZE) of
        {ok, Bin} ->
            binary_to_item_header(Bin);
        Else ->
            Else
    end.

item_header_to_binary(#item_header{cookie=Cookie,len=Len,flags=Flags}) ->
    <<?ITEM_HEADER_MAGIC:64/integer, ?ITEM_HEADER_VERSION:16/integer, Cookie:16/binary,
      Flags:16/integer, Len:64/integer>>.

binary_to_item_header(Bin) ->
    case Bin of
        <<?ITEM_HEADER_MAGIC:64/integer, ?ITEM_HEADER_VERSION:16/integer,
          Cookie:16/binary, Flags:16/integer, Len:64/integer>> ->
            {ok, #item_header{cookie=Cookie,len=Len,flags=Flags}};
        _ ->
            {error, invalid_item_header}
    end.
