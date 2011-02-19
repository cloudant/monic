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

-module(monic_file_manager).
-export([init/1,
    allowed_methods/2,
    content_types_accepted/2,
    content_types_provided/2,
    delete_resource/2,
    is_conflict/2,
    resource_exists/2]).
    
-export([from_json/2, to_json/2]).

-include_lib("webmachine/include/webmachine.hrl").

allowed_methods(ReqData, Context) ->
    {['DELETE', 'GET', 'PUT'], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{"application/json", from_json}], ReqData, Context}.
    
content_types_provided(ReqData, Context) ->
    {[{"application/json", to_json}], ReqData, Context}.
    
delete_resource(ReqData, Context) ->
    case file:delete(path(ReqData, Context)) of
        ok ->
            {true, ReqData, Context};
        _ ->
            {false, ReqData, Context}
    end.

init(ConfigProps) ->
    {ok, ConfigProps}.

is_conflict(ReqData, Context) ->  
    {exists(ReqData, Context), ReqData, Context}.

resource_exists(ReqData, Context) ->    
    {exists(ReqData, Context), ReqData, Context}.

from_json(ReqData, Context) ->
    ok = file:write_file(path(ReqData, Context), <<>>, [exclusive]),
    {true, ReqData, Context}.
    
to_json(ReqData, Context) ->
    {"{\"ok\": true}", ReqData, Context}.

path(ReqData, Context) ->
    Root = proplists:get_value(root, Context, "tmp"),
    File = wrq:path_info(file, ReqData),
    filename:join(Root, File).

exists(ReqData, Context) ->
    filelib:is_file(path(ReqData, Context)).

