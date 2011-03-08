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

-module(monic_file_writer_test).
-include("monic.hrl").
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    {foreach,
    fun setup/0,
    fun cleanup/1,
    [fun write/1,
    fun overflow/1
    ]}.

setup() ->
    file:delete("foo.monic"),
    file:delete("foo.monic.idx"),
    {ok, Pid} = monic_file_writer:open("foo.monic"),
    Pid.

cleanup(Pid) ->
    monic_file_writer:close(Pid).

write(Pid) ->
    Res = monic_file_writer:write(Pid, 3, fun(_Max) -> {ok, <<"123">>} end),
    ?_assertMatch({ok, 0, _}, Res).

overflow(Pid) ->
    Res = monic_file_writer:write(Pid, 3, fun(_Max) -> {ok, <<"1234">>} end),
    ?_assertEqual({error, overflow}, Res).
