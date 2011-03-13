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

%% Remove least recently used files when capacity is reached.

-module(monic_file_lru).
-behavior(gen_server).

%% public API
-export([start_link/1, update/1]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
          by_time,
          by_file,
          capacity
         }).

%% public functions.

start_link(Capacity) when is_integer(Capacity) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Capacity, []).

update(File) when is_pid(File) ->
    gen_server:call(?MODULE, {update, File}).

%% gen_server callbacks

init({0, _}) ->
    {stop, capacity_too_low};
init(Capacity) ->
    State = #state{
      capacity = Capacity,
      by_file = ets:new(monic_by_file, [set, private]),
      by_time = ets:new(monic_by_time, [ordered_set, private])
     },
    {ok, State}.

handle_call({update, File}, _From, State) ->
    maybe_evict_files(State),
    update_file(File, State),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{by_file=ByFile,by_time=ByTime}) ->
    lists:foreach(fun({File, _}) -> monic_file:close(File) end, ets:tab2list(ByFile)),
    ets:delete(ByFile),
    ets:delete(ByTime).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_evict_files(#state{by_file=ByFile, by_time=ByTime,
                         capacity=Capacity}) ->
    case ets:info(ByFile, size) of
        Capacity ->
            [{OldestTime, OldestFile}] = ets:lookup(ByTime, ets:first(ByTime)),
            true = ets:delete(ByFile, OldestFile),
            true = ets:delete(ByTime, OldestTime),
            monic_file:close(OldestFile);
        _ ->
            ok
    end.

update_file(File, #state{by_file=ByFile, by_time=ByTime}) ->
    case ets:lookup(ByFile, File) of
        [{_, PrevTime}] ->
            true = ets:delete(ByTime, PrevTime);
        [] ->
            ok
    end,
    Now = now(),
    true = ets:insert(ByFile, {File, Now}),
    true = ets:insert(ByTime, {Now, File}).
