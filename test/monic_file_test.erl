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

-module(monic_file_test).
-include("monic.hrl").
-include_lib("eunit/include/eunit.hrl").

basic_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun instantiate/1}.

setup() ->
    {ok, {_, Pid}} = monic_file:start_link("bar"),
    Pid.

cleanup(Pid) ->
    monic_file:close(Pid).

instantiate(Pid) ->
    Bin = <<"hello this is a quick test">>,
    {ok, Handle} = monic_file:write(Pid, Bin),
    {ok, Handle1} = monic_file:write(Pid, Bin),
    {ok, Bin1} = monic_file:read(Pid, Handle),
    Error = monic_file:read(Pid, Handle#handle{cookie="foo"}),
    [?_assertEqual(Bin, Bin1),
     ?_assertEqual(Handle#handle.uuid, Handle1#handle.uuid),
     ?_assertNot(Handle#handle.location == Handle1#handle.location),
     ?_assertNot(Handle#handle.cookie == Handle1#handle.cookie),
     ?_assertEqual({error,invalid_cookie}, Error)].

