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

-module(monic_utils).
-export([path/2, exists/2, open/2]).
-export([write_term/2, pread_term/2]).

-include("monic.hrl").

-define(MAX_TERM, (1 bsl 16)).

path(ReqData, Context) ->
    Root = proplists:get_value(root, Context, "tmp"),
    File = wrq:path_info(file, ReqData),
    filename:join(Root, File).

open(ReqData, Context) ->
    case monic_file:open(path(ReqData, Context)) of
        {ok, Pid} ->
            monic_file_lru:update(Pid),
            {ok, Pid};
        Else ->
            Else
    end.

exists(ReqData, Context) ->
    filelib:is_file(path(ReqData, Context)).

-spec write_term(term(), term()) -> {ok, integer()} | {error, term()}.
write_term(Fd, Term) ->
    Bin = term_to_binary(Term),
    Size = iolist_size(Bin),
    case Size =< ?MAX_TERM of
        true ->
            case file:write(Fd, <<Size:16/integer, Bin/binary>>) of
                ok ->
                    {ok, Size + 2};
                Else ->
                    Else
            end;
        false ->
            {error, term_too_long}
    end.

-spec pread_term(term(), integer()) -> {ok, integer(), term()} | eof | {error, term()}.
pread_term(Fd, Location) ->
    case file:pread(Fd, Location, 2) of
        {ok, <<Size:16/integer>>} ->
            case file:pread(Fd, Location + 2, Size) of
                {ok, <<Bin:Size/binary>>} ->
                    {ok, Size + 2, binary_to_term(Bin)};
                {ok, _} ->
                    eof;
                Else ->
                    Else
            end;
        Else ->
            Else
    end.
