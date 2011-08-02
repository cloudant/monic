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
    content_types_accepted/2,
    content_types_provided/2,
    delete_resource/2,
    last_modified/2,
    resource_exists/2,
    valid_entity_length/2]).
-export([get_item/2, put_item/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include("monic.hrl").

-define(ITEM_MIME_TYPE, "application/octet-stream").

init(ConfigProps) ->
    {ok, ConfigProps}.

allowed_methods(ReqData, Context) ->
    {['DELETE', 'GET', 'PUT'], ReqData, Context}.

content_types_provided(ReqData, Context) ->
    {[{?ITEM_MIME_TYPE, get_item}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{?ITEM_MIME_TYPE, put_item}], ReqData, Context}.

delete_resource(ReqData, Context) ->
    Key = wrq:path_info(key, ReqData),
    Cookie = list_to_integer(wrq:path_info(cookie, ReqData)),
    Result = case monic_utils:open(ReqData, Context) of
        {ok, Pid} ->
            monic_file:delete(Pid, Key, Cookie);
        _ ->
            false
    end,
    {Result, ReqData, Context}.

last_modified(ReqData, Context) ->
    Key = wrq:path_info(key, ReqData),
    Cookie = list_to_integer(wrq:path_info(cookie, ReqData)),
    LastModified1 = case monic_utils:open(ReqData, Context) of
        {ok, Pid} ->
            case monic_file:info(Pid, Key, Cookie) of
                {ok, #header{last_modified=LastModified}} ->
                    LastModified;
                {error, not_found} ->
                    undefined
            end;
        _ ->
            undefined
    end,
    {LastModified1, ReqData, Context}.

resource_exists(ReqData, Context) ->
    Key = wrq:path_info(key, ReqData),
    Cookie = list_to_integer(wrq:path_info(cookie, ReqData)),
    Exists = case monic_utils:open(ReqData, Context) of
        {ok, Pid} ->
            case monic_file:info(Pid, Key, Cookie) of
                {ok, _} ->
                    true;
                {error, not_found} ->
                    false
            end;
        _ ->
            false
    end,
    {Exists, ReqData, Context}.

valid_entity_length(ReqData, Context) ->
     Valid = case wrq:method(ReqData) of
        'PUT' ->
            wrq:get_req_header("Content-Length", ReqData) /= undefined;
        _ ->
            true
    end,
    {Valid, ReqData, Context}.

%% private functions

get_item(ReqData, Context) ->
    Key = wrq:path_info(key, ReqData),
    Cookie = list_to_integer(wrq:path_info(cookie, ReqData)),
    case monic_utils:open(ReqData, Context) of
        {ok, Pid} ->
            {ok, StreamBody} = monic_file:read(Pid, Key, Cookie),
            {{stream, StreamBody}, ReqData, Context};
        _ ->
            {<<>>, ReqData, Context}
    end.

put_item(ReqData, Context) ->
    Key = wrq:path_info(key, ReqData),
    Cookie = list_to_integer(wrq:path_info(cookie, ReqData)),
    case monic_utils:open(ReqData, Context) of
        {ok, Pid} ->
            Size = list_to_integer(wrq:get_req_header("Content-Length", ReqData)),
            StreamBody = wrq:stream_req_body(ReqData, ?BUFFER_SIZE),
            case monic_file:add(Pid, Key, Cookie, Size, StreamBody) of
                ok ->
                    File = wrq:path_info(file, ReqData),
                    Location = io_lib:format("/~s/~B/~s",[File, Cookie, Key]),
                    ReqData1 = wrq:set_resp_header("Location", Location, ReqData),
                    {true, ReqData1, Context};
                _ ->
                    {false, ReqData, Context}
            end;
        _ ->
            {false, ReqData, Context}
    end.
