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
      {timeout, 30, fun() -> write_bin() end},
      {timeout, 30, fun() -> write_fun() end},
      {timeout, 30, fun() -> unique_cookies() end},
      {timeout, 30, fun() -> unforgeable_cookie() end},
      {timeout, 30, fun() -> balanced_writers() end}
     ]}.

setup() ->
    monic:start().

cleanup(_) ->
    monic:stop().

write_bin() ->
    Bin = <<"hello this is a quick test">>,
    {ok, Handle} = monic:write("foo", Bin),
    {ok, Bin1} = monic:read("foo", Handle),
    ?assertEqual(Bin, Bin1).

write_fun() ->
    Fun = fun(_) ->
                  case get(next) of
                      undefined ->
                          put(next, {ok, <<"foobar">>});
                      {ok, _} ->
                          put(next, eof)
                  end,
                  get(next) end,
    {ok, Handle} = monic:write("foo", Fun),
    {ok, Bin1} = monic:read("foo", Handle),
    ?assertEqual(<<"foobar">>, Bin1).

unique_cookies() ->
    Bin = <<"hello this is a quick test">>,
    {ok, Handle1} = monic:write("foo", Bin),
    {ok, Handle2} = monic:write("foo", Bin),
    ?assertNot(Handle1#handle.cookie == Handle2#handle.cookie).

balanced_writers() ->
    Bin = <<"hello this is a quick test">>,
    {ok, Handle1} = monic:write("foo", Bin),
    {ok, Handle2} = monic:write("foo", Bin),
    ?assertNot(Handle1#handle.uuid == Handle2#handle.uuid).

unforgeable_cookie() ->
    Bin = <<"hello this is a quick test">>,
    {ok, Handle} = monic:write("foo", Bin),
    ?assertEqual({ok, Bin}, monic:read("foo", Handle)),
    ?assertEqual({error,invalid_cookie},
                 monic:read("foo", Handle#handle{cookie="foo"})).
