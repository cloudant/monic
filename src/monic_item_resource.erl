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

-module(monic_item_resource).
-export([init/1,
    allowed_methods/2,
    content_types_provided/2,
    resource_exists/2]).
-export([fetch/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include("monic.hrl").

allowed_methods(ReqData, Context) ->
    {['GET'], ReqData, Context}.

content_types_provided(ReqData, Context) ->
    {[{"application/octet-stream", fetch}], ReqData, Context}.

init(ConfigProps) ->
    {ok, ConfigProps}.

resource_exists(ReqData, Context) ->
    Key = list_to_integer(wrq:path_info(key, ReqData)),
    Cookie = list_to_integer(wrq:path_info(cookie, ReqData)),
    Exists = case monic_file:open(monic_utils:path(ReqData, Context)) of
        {ok, Pid} ->
            try
                case monic_file:lookup(Pid, Key, Cookie) of
                    {ok, _, _} ->
                        true;
                    {error, not_found} ->
                        false
                end
            after
                monic_file:close(Pid)
            end;
        _ ->
            false
    end,
    {Exists, ReqData, Context}.

fetch(ReqData, Context) ->
    Path = monic_utils:path(ReqData, Context),
    Key = list_to_integer(wrq:path_info(key, ReqData)),
    Cookie = list_to_integer(wrq:path_info(cookie, ReqData)),
    case monic_file:open(Path) of
        {ok, Pid} ->
            try
                StreamBody = case monic_file:lookup(Pid, Key, Cookie) of
                    {ok, Location, Size} ->
                        case file:open(Path, [read]) of
                            {ok, Fd} ->
                                {stream, pump(Fd, Location, Size)};
                            _ ->
                                <<>>
                        end;
                    {error, not_found} ->
                        <<>>
                end,
                {StreamBody, ReqData, Context}
            after
                monic_file:close(Pid)
            end;
        _ ->
            {<<>>, ReqData, Context}
    end.

pump(Fd, Location, Remaining) ->
    {ok, Bin} = file:pread(Fd, Location, min(Remaining, ?BUFFER_SIZE)),
    Size = iolist_size(Bin),
    case Size == Remaining of
        true ->
            {Bin, done};
        false ->
            {Bin, fun() -> pump(Fd, Location + Size, Remaining - Size) end}
    end.
