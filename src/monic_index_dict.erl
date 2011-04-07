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

%% Dict-backed index.

-module(monic_index_dict).
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

%% gen_server functions.

init(_) ->
    {ok, dict:new()}.

handle_call({insert, #header{key=Key}=Header}, _From, Dict) ->
    {reply, ok, dict:store(Key, Header, Dict)};

handle_call({lookup, Key}, _From, Dict) ->
    Reply = case dict:find(Key, Dict) of
                {ok, Value} ->
                    {ok, Value};
                error ->
                    {error, not_found}
            end,
    {reply, Reply, Dict};

handle_call({delete, Key}, _From, Dict) ->
    {reply, ok, dict:erase(Key, Dict)};

handle_call(stop, _From, Dict) ->
    {stop, normal, ok, Dict}.

handle_cast(_Msg, Dict) ->
    {noreply, Dict}.

handle_info(_Info, Dict) ->
    {noreply, Dict}.

terminate(_Reason, _Dict) ->
    ok.

code_change(_OldVsn, Dict, _Extra) ->
    {ok, Dict}.
