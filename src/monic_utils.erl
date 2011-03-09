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

-module(monic_utils).
-export([path/2, exists/2]).
-export([pread_header/2, pwrite_header/3]).
-export([pread_footer/2, pwrite_footer/3]).
-export([read_index/1, write_index/2]).
-export([new_cookie/0]).

-include("monic.hrl").

-define(ITEM_HEADER_MAGIC, 16#0f0f0f0f).
-define(ITEM_FOOTER_MAGIC, 16#7f7f7f7f).

path(ReqData, Context) ->
    Root = proplists:get_value(root, Context, "tmp"),
    File = wrq:path_info(file, ReqData),
    filename:join(Root, File).

exists(ReqData, Context) ->
    filelib:is_file(path(ReqData, Context)).

pwrite_header(Fd, Location, #header{key=Key,cookie=Cookie,size=Size,
    version=Version,flags=Flags}) ->
    Bin = <<?ITEM_HEADER_MAGIC:32/integer, Key:64/integer, Cookie:32/integer,
        Size:64/integer, Version:16/integer, Flags:16/bitstring>>,
    file:pwrite(Fd, Location, Bin).

pread_header(Fd, Location) ->
    case file:pread(Fd, Location, ?HEADER_SIZE) of
        {ok, <<?ITEM_HEADER_MAGIC:32/integer, Key:64/integer, Cookie:32/integer,
            Size:64/integer, Version:16/integer, Flags:16/bitstring>>} ->
            {ok, #header{key=Key,cookie=Cookie,size=Size,version=Version,flags=Flags}};
        {ok, _} ->
            {error, invalid_header};
        Else ->
            Else
    end.

pwrite_footer(Fd, Location, #footer{sha=Sha}) ->
    Bin = <<?ITEM_FOOTER_MAGIC:32/integer, Sha:20/binary>>,
    file:pwrite(Fd, Location, Bin).

pread_footer(Fd, Location) ->
    case file:pread(Fd, Location, ?FOOTER_SIZE) of
        {ok, <<?ITEM_FOOTER_MAGIC:32/integer, Sha:20/binary>>} ->
            {ok, #footer{sha=Sha}};
        {ok, _} ->
            {error, invalid_footer};
        Else ->
            Else
    end.

write_index(Fd, #index{key=Key,location=Location,size=Size,version=Version,flags=Flags}) ->
    Bin = <<Key:64/integer, Location:64/integer, Size:64/integer,
        Version:16/integer, Flags:16/bitstring>>,
    file:write(Fd, Bin).

read_index(Fd) ->
    case file:read(Fd, ?INDEX_SIZE) of
        {ok, <<Key:64/integer, Location:64/integer, Size:64/integer,
            Version:16/integer, Flags:16/bitstring>>} ->
                {ok, #index{key=Key,location=Location,size=Size,
                    version=Version,flags=Flags}};
        {ok, _} ->
            {error, invalid_index};
        Else ->
            Else
    end.

new_cookie() ->
    crypto:rand_uniform(0, 1 bsl 32).
