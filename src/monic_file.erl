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

-export([load/1, add/2]).

-define(ITEM_INDEX_SIZE, 28).
-define(ITEM_HEADER_SIZE, 24).
-define(ITEM_FOOTER_SIZE, 24).
-define(ONE_MILLION, 1000000).

load(Path) ->
    Tid = ets:new(index, []),
    case load_index(Tid, Path) of
        {ok, LastLocation} ->
            load_main(Tid, Path, LastLocation),
            {ok, Tid};
        Else ->
            Else
    end.

load_index(Tid, Path) ->
    case file:open(Path ++ ".idx", [binary, raw, read, read_ahead]) of
        {ok, Fd} ->
            Res = load_index_items(Tid, Fd, 0),
            file:close(Fd),
            Res;
        {error, enoent} ->
            {ok, 0};
        Else ->
            Else
    end.

load_index_items(Tid, Fd, LastLocation) ->
    case file:read(Fd, ?ITEM_INDEX_SIZE) of
        {ok, <<Key:64/integer, Location:64/integer, Size:64/integer, Version:16/integer, Deleted:1, _:15>>} ->
            case Deleted of
                0 -> ets:insert(Tid, {Key, Location, Size, LastModified});
                1 -> ok
            end,
            load_index_items(Tid, Fd, Location);
        eof ->
            {ok, LastLocation};
        Else ->
            Else
    end.

load_main(Tid, Path, LastLocation) ->
    case file:open(Path, [binary, raw, read, read_ahead]) of
        {ok, Fd} ->
            Res = load_main_items(Tid, Fd, LastLocation),
            file:close(Res),
            Res;
        Else ->
            Else
    end.

load_main_items(Tid, Fd, Location) ->
    case file:pread(Fd, Location, ?ITEM_HEADER_SIZE) of
        {ok, <<Key:64/integer, _Cookie:32/integer, Size:64/integer, Version:16/integer,  Deleted:1, _:15>>} ->
            case Deleted of
                0 -> ets:insert(Tid, {Key, Location, Size, Version});
                1 -> ok
            end,
            load_main_items(Tid, Fd, Location + Size + ?ITEM_HEADER_SIZE + ?ITEM_FOOTER_SIZE);
        eof ->
            ok;
        Else ->
            Else
    end.

add(Path, Bin) when is_binary(Bin) ->
    case file:open(Path, [append, binary, raw]) of
        {ok, Fd} ->
            Res = add_bin(Fd, Bin),
            ok = file:sync(Fd),
            file:close(Fd),
            Res;
        Else ->
            Else
    end.

add_bin(Fd, Bin) ->
    Key = 1, %% auto-increment.
    Cookie = crypto:rand_bytes(4),
    Size = iolist_size(Bin),
    ok = file:write(Fd, <<Key:64/integer, Cookie/binary, 0:1, Size:63/integer>>),
    ok = file:write(Fd, Bin),
    {ok, Key, Cookie}.

instant() ->
    {MegaSecs, Secs, _} = now(),
    (MegaSecs * ?ONE_MILLION * ?ONE_MILLION) + (Secs * 1000).
