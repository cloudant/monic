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
    {ok, #state{idle=Workers}}.

handle_call(Msg, From, State) ->
    {noreply, enqueue({Msg, From}, State)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Ref, Reply}, #state{idle=Idle,active=Active,reqs=Reqs}=State) ->
    case lists:keyfind(Ref, 1, Reqs) of
        {Ref, From, Worker} ->
            erlang:demonitor(Ref, [flush]),
            gen_server:reply(From, Reply),
            Reqs2 = lists:keydelete(Ref, 2, Reqs),
            {noreply, make_next_request(State#state{
                                          reqs=Reqs2,
                                          idle=Idle ++ [Worker],
                                          active = [A || A <- Active, A /= Worker]
                                         })};
        false ->
            {noreply, maybe_submit_request(State)}
    end;
handle_info({'DOWN', Ref, _, _, Reason}, #state{idle=Idle,active=Active,reqs=Reqs}=State) ->
    case lists:keyfind(Ref, 1, Reqs) of
        {Ref, From, Worker} ->
            gen_server:reply(From, {'EXIT', Reason}),
            Reqs2 = lists:keydelete(Ref, 2, Reqs),
            {noreply, make_next_request(State#state{
                                          reqs=Reqs2,
                                          idle=Idle ++ [Worker],
                                          active = [A || A <- Active, A /= Worker]
                                         })};
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

maybe_submit_request(#state{idle=[]}=State) ->
    State;
maybe_submit_request(#state{idle=[_Worker|_]} = State) ->
    make_next_request(State).

make_next_request(#state{idle=[Worker|Rest],active=Active,channel=Channel,reqs=Reqs}=State) ->
    case queue:out(Channel) of
        {{value, {Msg, From}}, Channel1} ->
            Ref = erlang:monitor(process, Worker),
            Worker ! {'$gen_call', {self(), Ref}, Msg},
            State#state{
              reqs=[{Ref, From, Worker} | Reqs],
              idle=Rest,
              active=[Worker] ++ Active,
              channel=Channel1};
        {empty, Channel} ->
            State
    end.
