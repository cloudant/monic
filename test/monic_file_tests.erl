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

-module(monic_file_tests).
-include("monic.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-define(COOKIE, 1).

all_test_() ->
    {foreach,
     fun() ->
             file:delete("foo.monic"),
             file:delete("foo.monic.idx"),
             file:delete("foo.monic.compact"),
             {ok, Pid} = monic_file:open("foo.monic"),
             Pid end,
     fun(Pid) -> monic_file:close(Pid) end,
     [
      fun add_single_hunk/1,
      fun add_multi_hunk/1,
      fun add_multi_items/1,
      fun large_item/1,
      fun overflow/1,
      fun underflow/1,
      fun update_item/1,
      fun delete_item/1,
      fun compaction/1,
      fun rebuild_index/1,
      fun repair_index/1
     ]}.

add_single_hunk(Pid) ->
    {"add an item in one hunk",
     fun() ->
             StreamBody = {<<"123">>, done},
             ?assertMatch(ok, monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, StreamBody)),
             ?assertMatch({ok, StreamBody}, monic_file:read(Pid, <<"foo">>,  ?COOKIE))
     end}.

add_multi_hunk(Pid) ->
    {"add an item in multiple hunks",
     fun() ->
             StreamBody = {<<"123">>, fun() -> {<<"456">>, done} end},
             ?assertMatch(ok, monic_file:add(Pid, <<"foo">>, ?COOKIE, 6, StreamBody))
     end}.

add_multi_items(Pid) ->
    [?_assertMatch(ok, monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, {<<"123">>, done})),
     ?_assertMatch(ok, monic_file:add(Pid, <<"bar">>, ?COOKIE, 3, {<<"456">>, done})),
     ?_assertMatch(ok, monic_file:add(Pid, <<"baz">>, ?COOKIE, 3, {<<"789">>, done})),
     ?_assertMatch(ok, monic_file:add(Pid, <<"foobar">>, ?COOKIE, 3, {<<"abc">>, done}))].

large_item(Pid) ->
    {"add a large item in one hunk",
     fun() ->
             Bin = crypto:rand_bytes(128*1024),
             StreamBody = {Bin, done},
             ?assertMatch(ok, monic_file:add(Pid, <<"foo">>, ?COOKIE, iolist_size(Bin), StreamBody)),
             {ok, StreamBody1} = monic_file:read(Pid, <<"foo">>, ?COOKIE),
             ?assertMatch(Bin, streambody_to_binary(StreamBody1))
     end}.

overflow(Pid) ->
    Res = monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, {<<"1234">>, done}),
    ?_assertEqual({error, overflow}, Res).

underflow(Pid) ->
    Res = monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, {<<"12">>, done}),
    ?_assertEqual({error, underflow}, Res).

update_item(Pid) ->
    {"update an item",
     fun() ->
             ?assertMatch(ok, monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, {<<"123">>, done})),
             ?assertMatch({ok, {<<"123">>, done}}, monic_file:read(Pid, <<"foo">>,  ?COOKIE)),
             ?assertMatch(ok, monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, {<<"456">>, done})),
             ?assertMatch({ok, {<<"456">>, done}}, monic_file:read(Pid, <<"foo">>,  ?COOKIE))
     end}.

delete_item(Pid) ->
    {"delete an item",
     fun() ->
             ?assertMatch(ok, monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, {<<"123">>, done})),
             ?assertMatch({ok, _}, monic_file:read(Pid, <<"foo">>,  ?COOKIE)),
             ?assertMatch(ok, monic_file:delete(Pid, <<"foo">>, ?COOKIE)),
             ?assertMatch({error, not_found}, monic_file:read(Pid, <<"foo">>,  ?COOKIE))
     end}.

compaction(Pid) ->
    {"compaction",
     fun() ->
             ok = monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, {<<"123">>, done}),
             ok = monic_file:add(Pid, <<"bar">>, ?COOKIE, 3, {<<"456">>, done}),
             ok = monic_file:delete(Pid, <<"foo">>, ?COOKIE),
             {ok, #file_info{size=BeforeSize}} = file:read_file_info("foo.monic"),
             ok = monic_file:compact(Pid),
             {ok, #file_info{size=AfterSize}} = file:read_file_info("foo.monic"),
             ?assert(AfterSize < BeforeSize),
             ?assertMatch({ok, {<<"456">>, done}}, monic_file:read(Pid, <<"bar">>,  ?COOKIE))
     end}.

rebuild_index(_) ->
    {"index is rebuilt if lost completely",
     fun() ->
             {ok, Pid} = monic_file:open("bar.monic"),
             ok = monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, {<<"123">>, done}),
             ok = monic_file:sync(Pid),
             {ok, OriginalIndex} = file:read_file("bar.monic.idx"),
             ok = monic_file:close(Pid),

             %% delete the index.
             ok = file:delete("bar.monic.idx"),

             %% verify the index is rebuilt.
             {ok, Pid1} = monic_file:open("bar.monic"),
             try
                 {ok, RebuiltIndex} = file:read_file("bar.monic.idx"),
                 ?assertEqual(OriginalIndex, RebuiltIndex)
             after
                 monic_file:close(Pid1),
                 file:delete("bar.monic"),
                 file:delete("bar.monic.idx")
             end
     end}.

repair_index(_) ->
    {"index is repaired if it exists but newer items exists in the main file",
     fun() ->
             {ok, Pid} = monic_file:open("bar.monic"),
             ok = monic_file:add(Pid, <<"foo">>, ?COOKIE, 3, {<<"123">>, done}),
             ok = monic_file:sync(Pid),
             {ok, #file_info{size=Size}} = file:read_file_info("bar.monic.idx"),
             ok = monic_file:add(Pid, <<"bar">>, ?COOKIE, 3, {<<"456">>, done}),
             ok = monic_file:sync(Pid),
             {ok, OriginalIndex} = file:read_file("bar.monic.idx"),
             ok = monic_file:close(Pid),

             %% truncate index by one whole record.
             {ok, Fd} = file:open("bar.monic.idx", [write]),
             {ok, _} = file:position(Fd, Size),
             ok = file:close(Fd),

             %% verify the entry is restored.
             {ok, Pid1} = monic_file:open("bar.monic"),
             try
                 {ok, RebuiltIndex} = file:read_file("bar.monic.idx"),
                 ?assertEqual(OriginalIndex, RebuiltIndex)
             after
                 monic_file:close(Pid1),
                 file:delete("bar.monic"),
                 file:delete("bar.monic.idx")
             end
     end}.

streambody_to_binary(StreamBody) ->
    iolist_to_binary(streambody_to_iolist(StreamBody)).

streambody_to_iolist(StreamBody) ->
    lists:reverse(streambody_to_iolist(StreamBody, [])).

streambody_to_iolist({Bin, done}, Acc) ->
    [Bin|Acc];
streambody_to_iolist({Bin, Next}, Acc) ->
    streambody_to_iolist(Next(), [Bin|Acc]).
