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

-module(monic_file_resource).
-export([init/1,
    allowed_methods/2,
    content_types_accepted/2,
    content_types_provided/2,
    create_path/2,
    delete_resource/2,
    is_conflict/2,
    post_is_create/2,
    resource_exists/2,
    valid_entity_length/2]).
-export([show_file/2, create_file/2, add_item/2]).

-include_lib("webmachine/include/webmachine.hrl").
-define(BUFFER_SIZE, 64*1024).

allowed_methods(ReqData, Context) ->
    {['DELETE', 'GET', 'PUT', 'POST'], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    case wrq:method(ReqData) of
        'PUT' ->
            {[{"application/json", create_file}], ReqData, Context};
        'POST' ->
            CT = case wrq:get_req_header("Content-Type", ReqData) of
                undefined -> "application/octet-stream";
                Other -> Other
            end,
            {[{CT, add_item}], ReqData, Context}
    end.

content_types_provided(ReqData, Context) ->
    {[{"application/json", show_file}], ReqData, Context}.

create_path(ReqData, Context) ->
    File = wrq:path_info(file, ReqData),
    Key = 1, %% TODO increment.
    Cookie = crypto:rand_uniform(1, 1 bsl 32),
    {io_lib:format("/~s/~B/~B", [File, Key, Cookie]), ReqData, Context}.

delete_resource(ReqData, Context) ->
    case file:delete(monic_utils:path(ReqData, Context)) of
        ok ->
            {true, ReqData, Context};
        _ ->
            {false, ReqData, Context}
    end.

init(ConfigProps) ->
    {ok, ConfigProps}.

is_conflict(ReqData, Context) ->
    {monic_utils:exists(ReqData, Context), ReqData, Context}.

post_is_create(ReqData, Context) ->
    {true, ReqData, Context}.

resource_exists(ReqData, Context) ->
    {monic_utils:exists(ReqData, Context), ReqData, Context}.

valid_entity_length(ReqData, Context) ->
    Valid = case wrq:method(ReqData) of
        'POST' ->
            wrq:get_req_header("Content-Length", ReqData) /= undefined;
        _ ->
            true
    end,    
    {Valid, ReqData, Context}.

%% private functions

show_file(ReqData, Context) ->
    {"{\"ok\": true}", ReqData, Context}.

create_file(ReqData, Context) ->
    case file:write_file(monic_utils:path(ReqData, Context), <<>>, [exclusive]) of
        ok -> {true, ReqData, Context};
        _ -> {false, ReqData, Context}
    end.

add_item(ReqData, Context) ->
    case monic_file:open(monic_utils:path(ReqData, Context)) of
        {ok, Pid} ->
            try
                Size = list_to_integer(wrq:get_req_header("Content-Length", ReqData)),
                StreamBody = wrq:stream_req_body(ReqData, ?BUFFER_SIZE),
                case monic_file:add(Pid, Size, StreamBody) of
                    {ok, Key, Cookie} ->
                        {true, ReqData, Context};
                    _ ->
                        {false, ReqData, Context}
                end
            after
                monic_file:close(Pid)
            end;
        _ ->
            {false, ReqData, Context}
    end.
