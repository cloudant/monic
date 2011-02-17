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

-module(monic_group).
-behaviour(gen_server).
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-include("monic.hrl").

-record(state, {
          active=[],
          idle=[],
          channel=queue:new(),
          reqs=[]
         }).

%% public functions

start_link(Group) ->
    gen_server:start_link(?MODULE, Group, []).

%% gen_server functions

init(Group) ->
    {ok, C} = application:get_env(monic, concurrency),
    filelib:ensure_dir(filename:join(Group, "foo")),
    Workers = [begin
                   Path = filename:join(Group, integer_to_list(N) ++ ".monic"),
                   {ok, W} = monic_file:start_link(Path),
                   W
               end || N <- lists:seq(1, C)],
    %% open all existing files too!
    {ok, #state{idle=Workers}}.

handle_call(Msg, From, State) ->
    {noreply, enqueue({Msg, From}, State)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Ref, Reply}, State) ->
    case return_worker(Ref, State) of
        {From, State1} ->
            erlang:demonitor(Ref, [flush]),
            gen_server:reply(From, Reply),
            {noreply, maybe_submit_request(State1)};
        false ->
            {noreply, maybe_submit_request(State)}
    end;
handle_info({'DOWN', Ref, _, _, Reason}, State) ->
    case return_worker(Ref, State) of
        {From, State1} ->
            gen_server:reply(From, {'EXIT', Reason}),
            {noreply, maybe_submit_request(State1)};
        false ->
            {noreply, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% private functions

enqueue(Request, #state{channel=Channel}=State) ->
    State1 = State#state{channel=queue:in(Request, Channel)},
    maybe_submit_request(State1).

maybe_submit_request(#state{idle=Idle,channel=Channel}=State) ->
    case queue:peek(Channel) of
        {value,{Msg,_}} ->
            case choose_worker(Msg, Idle) of
                false ->
                    State;
                Worker ->
                    make_next_request(Worker, State)
            end;
        empty ->
            State
    end.

make_next_request({_, Pid}=Worker, #state{idle=Idle,active=Active,channel=Channel,reqs=Reqs}=State) ->
    case queue:out(Channel) of
        {{value, {Msg, From}}, Channel1} ->
            Ref = erlang:monitor(process, Pid),
            Pid ! {'$gen_call', {self(), Ref}, Msg},
            State#state{
              reqs=[{Ref, From, Worker} | Reqs],
              idle=[I || I <- Idle, I /= Worker],
              active=[Worker] ++ Active,
              channel=Channel1};
        {empty, Channel} ->
            State
    end.

choose_worker(_Request, []) ->
    false;
choose_worker({write, _}, [H|_]) ->
    H;
choose_worker({read, #handle{uuid=UUID}}, Workers) ->
    choose_worker_by_uuid(UUID, Workers);
choose_worker({read, #handle{uuid=UUID}, _}, Workers) ->
    choose_worker_by_uuid(UUID, Workers);
choose_worker({read, #handle{uuid=UUID}, _, _}, Workers) ->
    choose_worker_by_uuid(UUID, Workers);
choose_worker({delete, #handle{uuid=UUID}}, Workers) ->
    choose_worker_by_uuid(UUID, Workers).

choose_worker_by_uuid(UUID, Workers) ->
    case lists:keysearch(UUID, 1, Workers) of
        {value, Worker} ->
            Worker;
        false ->
            false
    end.

return_worker(Ref, #state{idle=Idle,active=Active,reqs=Reqs}=State) ->
    case lists:keyfind(Ref, 1, Reqs) of
        {Ref, From, Worker} ->
            Reqs1 = lists:keydelete(Ref, 2, Reqs),
            {From,
             State#state{
               reqs=Reqs1,
               idle=Idle ++ [Worker],
               active = [A || A <- Active, A /= Worker]
              }};
        false ->
            false
    end.
