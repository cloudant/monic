% Copyright 2011 Cloudant
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(monic_file).
-include("monic.hrl").

% public API
-export([start_link/1, close/1, write/2, read/2]).

% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {fd, eof}).

-define(VSN, 1).

-record(header, {cookie, flags=0, len}).
-define(HEADER_SIZE, 36).
-define(HEADER_MAGIC, <<101,128,65,118,187,134,229,200>>).

-record(footer, {checksum}).
-define(FOOTER_SIZE, 28).
-define(FOOTER_MAGIC, <<150,22,117,49,231,170,49,181>>).

% public functions

start_link(Path) ->
    gen_server:start_link(?MODULE, Path, []).

close(Pid) ->
    gen_server:call(Pid, close, infinity).

write(Pid, Bin) when is_binary(Bin) ->
    gen_server:call(Pid, {write, Bin}, infinity).

read(Pid, #handle{}=Handle) ->
    gen_server:call(Pid, {read, Handle}, infinity).

% gen_server functions

init(Path) ->
    case file:open(Path, [read, write, raw, binary]) of
        {ok, Fd} ->
            {ok, Eof} = file:position(Fd, eof),
            {ok, #state{eof=Eof,fd=Fd}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({write, Bin}, _From, #state{eof=Eof,fd=Fd}=State) ->
    Cookie = crypto:rand_bytes(16),
    Header = #header{cookie=Cookie, len=iolist_size(Bin)},
    Bin1 = [header_to_binary(Header), Bin],
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
    case read_header(Fd, Location) of
        {ok, #header{cookie=Cookie,len=Len}} ->
            {reply, file:pread(Fd, Location+?HEADER_SIZE, Len), State};
        {ok, #header{cookie=Cookie1}} ->
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

header_to_binary(#header{cookie=Cookie,len=Len,flags=Flags}) ->
    <<?HEADER_MAGIC:8/binary, ?VSN:16/integer, Cookie:16/binary,
      Flags:16/integer, Len:64/integer>>.

binary_to_header(Bin) ->
    <<_Magic:8/binary, _Version:16/integer, Cookie:16/binary,
      Flags:16/integer, Len:64/integer>> = Bin,
    #header{cookie=Cookie,len=Len,flags=Flags}.

read_header(Fd, Location) ->
    case file:pread(Fd, Location, ?HEADER_SIZE) of
        {ok, Bin} ->
            {ok, binary_to_header(Bin)};
        Else ->
            Else
    end.
