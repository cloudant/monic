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
-behavior(gen_server).
-include("monic.hrl").

%% public API

-export([start_link/0, stop/1]).
-export([insert/2, delete/2, lookup/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

-spec insert(pid(), #header{}) -> ok.
insert(Pid, #header{}=Header) ->
    gen_server:call(Pid, {insert, Header}).

-spec delete(pid(), term()) -> ok.
delete(Pid, Key) ->
    gen_server:call(Pid, {delete, Key}).

-spec lookup(pid(), term()) -> {ok, #header{}} | {error, not_found}.
lookup(Pid, Key) ->
    gen_server:call(Pid, {lookup, Key}).

init(_) ->
    {ok, ets:new(index, [{keypos, #header.key}, set, private])}.

handle_call({insert, Header}, _From, Tid) ->
    true = ets:insert(Tid, Header),
    {reply, ok, Tid};

handle_call({lookup, Key}, _From, Tid) ->
    Reply = case ets:lookup(Tid, Key) of
                [#header{}=Header] ->
                    {ok, Header};
                _ ->
                    {error, not_found}
            end,
    {reply, Reply, Tid};

handle_call({delete, Key}, _From, Tid) ->
    true = ets:delete(Tid, Key),
    {reply, ok, Tid};

handle_call(stop, _From, Tid) ->
    ets:delete(Tid),
    {stop, normal, ok, nil}.

handle_cast(_Msg, Tid) ->
    {noreply, Tid}.

handle_info(_Info, Tid) ->
    {noreply, Tid}.

terminate(_Reason, nil) ->
    ok;
terminate(_Reason, Tid) ->
    ets:delete(Tid).

code_change(_OldVsn, Tid, _Extra) ->
    {ok, Tid}.
