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

%% Remove least recently used items when capacity is reached.

-module(monic_lru).
-behavior(gen_server).

%% public API
-export([start_link/2, update/2]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
    by_time,
    by_item,
    capacity,
    eviction_fun
}).

%% public functions.

start_link(Capacity, EvictionFun) when is_function(EvictionFun) ->
    gen_server:start_link(?MODULE, {Capacity, EvictionFun}, []).

%% gen_server callbacks

init({0, _}) ->
    {stop, capacity_too_low};
init({Capacity, EvictionFun}) ->
    State = #state{
        capacity = Capacity,
        eviction_fun = EvictionFun,
        by_item = ets:new(monic_by_item, [set, private]),
        by_time = ets:new(monic_by_time, [ordered_set, private])
    },
    {ok, State}.

update(Pid, Item) ->
    gen_server:call(Pid, {update, Item}).

handle_call({update, Item}, _From, State) ->
    maybe_evict_items(State),
    update_item(Item, State),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{by_item=ByItem,by_time=ByTime}) ->
    ets:delete(ByItem),
    ets:delete(ByTime).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

maybe_evict_items(#state{by_item=ByItem, by_time=ByTime,
    capacity=Capacity, eviction_fun=EvictionFun}) ->
    case ets:info(ByItem, size) of
        Capacity ->
            [{OldestTime, OldestItem}] = ets:lookup(ByTime, ets:first(ByTime)),
            true = ets:delete(ByItem, OldestItem),
            true = ets:delete(ByTime, OldestTime),
            EvictionFun(OldestItem);
        _ ->
            ok
    end.

update_item(Item, #state{by_item=ByItem, by_time=ByTime}) ->
    case ets:lookup(ByItem, Item) of
        [{_, PrevTime}] ->
            true = ets:delete(ByTime, PrevTime);
        [] ->
            ok
    end,
    Now = now(),
    true = ets:insert(ByItem, {Item, Now}),
    true = ets:insert(ByTime, {Now, Item}).
