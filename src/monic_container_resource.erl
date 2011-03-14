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

-module(monic_container_resource).
-export([init/1,
         allowed_methods/2,
         content_types_accepted/2,
         content_types_provided/2,
         delete_resource/2,
         is_conflict/2,
         resource_exists/2]).
-export([get_container/2, put_container/2]).

-include_lib("webmachine/include/webmachine.hrl").
-include("monic.hrl").

-define(CONTAINER_MIME_TYPE, "application/vnd.com.cloudant.monic.container+json").

init(ConfigProps) ->
    {ok, ConfigProps}.

allowed_methods(ReqData, Context) ->
    {['DELETE', 'GET', 'PUT'], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{?CONTAINER_MIME_TYPE, put_container}], ReqData, Context}.

content_types_provided(ReqData, Context) ->
    {[{?CONTAINER_MIME_TYPE, get_container}], ReqData, Context}.

delete_resource(ReqData, Context) ->
    {file:delete(monic_utils:path(ReqData, Context)) == ok, ReqData, Context}.

is_conflict(ReqData, Context) ->
    {monic_utils:exists(ReqData, Context), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {monic_utils:exists(ReqData, Context), ReqData, Context}.

%% private functions

get_container(ReqData, Context) ->
    {"{\"ok\": true}", ReqData, Context}.

put_container(ReqData, Context) ->
    {file:write_file(monic_utils:path(ReqData, Context), <<>>, [exclusive]) == ok, ReqData, Context}.
