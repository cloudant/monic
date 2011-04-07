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

%% ETS-backed index. other implementations to follow.

-module(monic_index_ets).
-include("monic.hrl").

%% public API

-export([new/0, delete/1]).
-export([insert/2, delete/2, lookup/2]).

new() ->
    ets:new(index, [{keypos, #header.key}, set, private]).

delete(Tid) ->
    ets:delete(Tid).

insert(Tid, #header{}=Header) ->
    ets:insert(Tid, Header).

delete(Tid, Key) ->
    ets:delete(Tid, Key).

lookup(Tid, Key) ->
    ets:lookup(Tid, Key).
