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

-module(monic_test).
-include("monic.hrl").
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
      {timeout, 30, fun() -> basic() end}
     ]}.

setup() ->
    monic:start().

cleanup(_) ->
    monic:stop().

basic() ->
    Group = "foo",
    Bin = <<"hello this is a quick test">>,
    {ok, Handle} = monic:write(Group, Bin),
    {ok, Bin1} = monic:read(Group, Handle),
    ?assertEqual(Bin, Bin1).

