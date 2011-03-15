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
-export([pwrite_term/3, pread_term/2, header_to_index/2]).

-include("monic.hrl").

-define(ITEM_HEADER_MAGIC, 16#0f0f0f0f).
-define(ITEM_FOOTER_MAGIC, 16#7f7f7f7f).

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

-spec pwrite_term(term(), integer(), term()) -> {ok, integer()} | {error, term()}.
pwrite_term(Fd, Location, Term) ->
    Bin = term_to_binary(Term),
    Size = iolist_size(Bin),
    case Size =< ?MAX_TERM of
        true ->
            case file:pwrite(Fd, Location, <<Size:16/integer, Bin/binary>>) of
                ok ->
                    {ok, Size + 2};
                Else ->
                    Else
            end;
        false ->
            {error, term_too_long}
    end.

-spec pread_term(term(), integer()) -> {ok, integer(), binary()} | eof | {error, term()}.
pread_term(Fd, Location) ->
    case file:pread(Fd, Location, ?MAX_TERM) of
        {ok, <<Size:16/integer, Bin/binary>>} ->
            {ok, Size + 2, binary_to_term(<<Bin:Size/binary>>)};
        Else ->
            Else
    end.

-spec header_to_index(#header{}, integer()) -> #index{}.
header_to_index(#header{key=Key,cookie=Cookie,size=Size,last_modified=LastModified, deleted=Deleted}, Location) ->
    #index{key=Key,cookie=Cookie,size=Size,last_modified=LastModified,deleted=Deleted,location=Location}.

