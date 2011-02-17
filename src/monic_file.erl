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
-export([start_link/1, close/1]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {uuid, fd, eof}).

-record(file_header, {uuid, eof}).
-define(FILE_HEADER_VERSION, 1).
-define(FILE_HEADER_SIZE, 4096).
-define(FILE_HEADER_MAGIC, 16067190828064835550).

-record(item_header, {cookie, flags=0, len}).
-define(ITEM_HEADER_VERSION, 1).
-define(ITEM_HEADER_SIZE, 36).
-define(ITEM_HEADER_MAGIC, 2998602972810243711).

-record(item_footer, {sha}).
-define(ITEM_FOOTER_SIZE, 28).
-define(ITEM_FOOTER_MAGIC, 15866124023828541222).

-define(BUFFER_SIZE, 16384).

%% public functions

start_link(Path) ->
    case gen_server:start_link({local, list_to_atom(Path)}, ?MODULE, Path, []) of
        {ok, Pid} ->
            UUID = gen_server:call(Pid, uuid),
            {ok, {UUID, Pid}};
        Else ->
            Else
    end.

close(Pid) ->
    gen_server:call(Pid, close, infinity).

%% gen_server functions

init(Path) ->
    case file:open(Path, [read, write, raw, binary]) of
        {ok, Fd} ->
            case get_file_header(Fd) of
                {ok, #file_header{eof=Eof,uuid=UUID}} ->
                    {ok, #state{uuid=UUID,eof=Eof,fd=Fd}};
                {error, Reason} ->
                    {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

%% kill the following clause.
handle_call({write, Bin}, _From, #state{uuid=UUID,eof=Eof,fd=Fd}=State) when is_binary(Bin) ->
    Cookie = crypto:rand_bytes(16),
    Header = #item_header{cookie=Cookie, len=iolist_size(Bin)},
    Footer = #item_footer{sha=crypto:sha(Bin)},
    Bin1 = [item_header_to_binary(Header), Bin, item_footer_to_binary(Footer)],
    Size1 = iolist_size(Bin1),
    case file:pwrite(Fd, Eof, Bin1) of
        ok ->
            case file:datasync(Fd) of
                ok ->
                    Eof1 = Eof + Size1,
                    Handle = #handle{location=Eof, uuid=UUID, cookie=Cookie},
                    write_file_header(Fd, #file_header{uuid=UUID, eof=Eof1}),
                    {reply, {ok, Handle}, State#state{eof=Eof1}};
                Else ->
                    {reply, Else, State}
            end;
        Else ->
            {reply, Else, State}
    end;
handle_call({write, Fun}, _From, #state{uuid=UUID,eof=Eof,fd=Fd}=State) when is_function(Fun) ->
    case stream_in(Fd, Fun, Eof + ?ITEM_HEADER_SIZE) of
        {ok, Eof1, Len, Sha} ->
            Cookie = crypto:rand_bytes(16),
            Header = #item_header{cookie=Cookie, len=Len},
            Footer = #item_footer{sha=Sha},
            case file:pwrite(Fd, Eof, item_header_to_binary(Header)) of
                ok ->
                    case file:pwrite(Fd, Eof1, item_footer_to_binary(Footer)) of
                        ok ->
                            Eof2 = Eof1 + ?ITEM_FOOTER_SIZE,
                            case file:datasync(Fd) of
                                ok ->
                                    Handle = #handle{location=Eof, uuid=UUID, cookie=Cookie},
                                    write_file_header(Fd, #file_header{uuid=UUID, eof=Eof2}),
                                {reply, {ok, Handle}, State#state{eof=Eof2}};
                                Else ->
                                    {reply, Else, State}
                            end;
                        Else ->
                            {reply, Else, State}
                    end;
                Else->
                    {reply, Else, State}
            end;
        Else->
            {reply, Else, State}
    end;
handle_call({read, #handle{uuid=UUID}}, _From, #state{uuid=UUID1}=State) when UUID /= UUID1 ->
    {reply, {error, wrong_file}, State};
%% kill the following clause.
handle_call({read, #handle{location=Location,cookie=Cookie}}, _From, #state{fd=Fd}=State) ->
    case read_item_header(Fd, Location) of
        {ok, #item_header{flags=1}} ->
            {reply, {error, deleted}, State};
        {ok, #item_header{cookie=Cookie,len=Len}} ->
            {reply, file:pread(Fd, Location+?ITEM_HEADER_SIZE, Len), State};
        {ok, #item_header{}} ->
            {reply, {error, invalid_cookie}, State};
        Else ->
            {reply, Else, State}
    end;
handle_call({read, Handle, Fun}, From, State) ->
    handle_call({read, Handle, [], Fun}, From, State);
handle_call({read, #handle{location=Location,cookie=Cookie}, Ranges, Fun}, _From, #state{fd=Fd}=State) ->
    case read_item_header(Fd, Location) of
        {ok, #item_header{flags=1}} ->
            {reply, {error, deleted}, State};
        {ok, #item_header{cookie=Cookie,len=Len}} ->
            Ranges1 = case Ranges of
                          [] -> [{0, Len}];
                          _ -> Ranges
                      end,
            case validate_ranges(Ranges1, Len) of
                ok ->
                    Start = Location + ?ITEM_HEADER_SIZE,
                    case lists:foldl(fun({RangeOff, RangeLen}, _) ->
                                             stream_out(Fd, Fun, Start + RangeOff, RangeLen)
                                     end, fail, Ranges1) of
                        {ok, CalculatedSha} ->
                            case read_item_footer(Fd, Location + ?ITEM_HEADER_SIZE + Len) of
                                {ok, #item_footer{sha=RecordedSha}} ->
                                    case {Ranges1, RecordedSha} of
                                        {[{0, Len}], CalculatedSha} -> Fun({checksum, valid});
                                        {[{0, Len}], _OtherSha} -> Fun({checksum, invalid});
                                        _ -> ok
                                    end,
                                    Fun(eof),
                                    {reply, ok, State};
                                Else ->
                                    {reply, Else, State}
                            end;
                        Else ->
                            {reply, Else, State}
                    end;
                Else ->
                    {reply, Else, State}
            end;
        {ok, #item_header{}} ->
            {reply, {error, invalid_cookie}, State};
        Else ->
            {reply, Else, State}
    end;
handle_call(close, _From, #state{fd=Fd}=State) ->
    {stop, normal, file:close(Fd), State#state{fd=nil}};
handle_call({delete, #handle{location=Location,cookie=Cookie}}, _From, #state{fd=Fd}=State) ->
    case read_item_header(Fd, Location) of
        {ok, #item_header{cookie=Cookie}=Header} ->
            Header1 = Header#item_header{flags=1},
            case file:pwrite(Fd, Location, item_header_to_binary(Header1)) of
                ok ->
                    {reply, ok, State};
                Else ->
                    {reply, Else, State}
            end;
        Else ->
            {reply, Else, State}
    end;
handle_call(uuid, _From, #state{uuid=UUID}=State) ->
    {reply, UUID, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{fd=nil}) ->
    ok;
terminate(_Reason, #state{fd=Fd}) ->
    file:close(Fd).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% private functions

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
    case write_file_header(Fd, NewHeader) of
        ok ->
            {ok, NewHeader};
        Else ->
            Else
    end.

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
    [Bin, <<0:(8*(?FILE_HEADER_SIZE - iolist_size(Bin)))>>].

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

item_footer_to_binary(#item_footer{sha=Sha}) ->
    <<?ITEM_FOOTER_MAGIC:64/integer, Sha:20/binary>>.

binary_to_item_footer(Bin) ->
    case Bin of
        <<?ITEM_FOOTER_MAGIC:64/integer,Sha:20/binary>> ->
            {ok, #item_footer{sha=Sha}};
        _ ->
            {error, invalid_item_footer}
    end.

read_item_footer(Fd, Location) ->
    case file:pread(Fd, Location, ?ITEM_FOOTER_SIZE) of
        {ok, Bin} ->
            binary_to_item_footer(Bin);
        Else ->
            Else
    end.

stream_in(Fd, Fun, Eof) ->
    stream_in(Fd, Fun, Eof, 0, crypto:sha_init()).

stream_in(Fd, Fun, Eof, Len, Sha) ->
    case Fun(?BUFFER_SIZE) of
        {ok, Bin} ->
            case file:pwrite(Fd, Eof, Bin) of
                ok ->
                    Size = iolist_size(Bin),
                    stream_in(Fd, Fun, Eof + Size, Len + Size,
                         crypto:sha_update(Sha, Bin));
                Error ->
                    Error
            end;
        eof ->
            {ok, Eof, Len, crypto:sha_final(Sha)};
        Error ->
            Error
    end.


stream_out(Fd, Fun, Location, Len) ->
    stream_out(Fd, Fun, Location, Len, crypto:sha_init()).

stream_out(_Fd, _Fun, _Location, 0, Sha) ->
    {ok, crypto:sha_final(Sha)};
stream_out(Fd, Fun, Location, Remaining, Sha) when Remaining > 0 ->
    case file:pread(Fd, Location, min(Remaining, ?BUFFER_SIZE)) of
        {ok, Bin} ->
            Size = iolist_size(Bin),
            Fun({ok, Bin}),
            stream_out(Fd, Fun, Location + Size, Remaining - Size,
                       crypto:sha_update(Sha, Bin));
        Else ->
            Else
    end.

validate_ranges([], _MaxLen) ->
    ok;
validate_ranges([{From, Len}|T], MaxLen) ->
    case From >= 0 andalso (From+Len) =< MaxLen of
        true ->
            validate_ranges(T, MaxLen);
        false ->
            {error, invalid_range}
    end.
