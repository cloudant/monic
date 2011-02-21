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
    create_path/2,
    delete_resource/2,
    post_is_create/2,
    resource_exists/2]).

allowed_methods(ReqData, Context) ->
    {['DELETE', 'GET', 'POST'], ReqData, Context}.

content_types_provided(ReqData, Context) ->
    {[{"application/octet-stream", to_binary}], ReqData, Context}.

create_path(ReqData, Context) ->
    {"/fixme", ReqData, Context}.

delete_resource(ReqData, Context) ->
    {false, ReqData, Context}.

init(ConfigProps) ->
    {ok, ConfigProps}.

post_is_create(ReqData, Context) ->
    {true, ReqData, Context}.

resource_exists(ReqData, Context) ->
    {true, ReqData, Context}.
