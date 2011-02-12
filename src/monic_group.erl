% Copyright 2011 Cloudant
%
% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(monic_group).

-include("monic.hrl").

% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
          busy=[],
          idle=[],
          requests=[]}).

% public functions

start_link(Name) ->
    gen_server:start_link({local, list_to_atom(Name)}, ?MODULE, Name, []).

% gen_server functions

init(Name) ->
    State = init_workers(Name),
    {ok, State}.

handle_call({read, #handle{location=Path}}=Request, From, #state{idle=Idle}=State)->
    case lists:partition(fun({Path1, _}) -> Path1 == Path end, Idle) of
        {[], _} ->
            {noreply, State#state{requests=[{Request, From} | State#state.requests]}};
        {[W], Rest} ->
            monic_worker:start_work(W, {Request, From}),
            {noreply, State#state{idle=Rest,busy=[W|State#state.busy]}}
    end;
handle_call({write, _Bin}=Request, From, #state{idle=Idle}=State) ->
    case Idle of
        [] ->
            {noreply, State#state{requests=[{Request, From} | State#state.requests]}};
        [W|Rest] ->
            monic_worker:start_work(W, {Request, From}),
            {noreply, State#state{idle=Rest,busy=[W|State#state.busy]}}
    end;
handle_call(close, _From, State) ->
    shutdown_workers(State),
    {stop, normal, ok, State}.

handle_cast({done, Worker, From, Resp}, #state{requests=[]}=State) ->
    Busy = [B || B <- State#state.busy, B /= Worker],
    Idle = State#state.idle ++ [Worker],
    gen_server:reply(From, Resp),
    {noreply, State#state{idle=Idle, busy=Busy}};
handle_cast({done, {_, Pid}, From, Resp}, #state{requests=[R|Rest]}=State) ->
    gen_server:reply(From, Resp),
    monic_worker:start_work(Pid, R),
    {noreply, State#state{requests=Rest}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    shutdown_workers(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% private functions

init_workers(Name) ->
    Concurrency = application:get_env(monic, concurrency),
    Workers = [begin
                   Path = filename:join(Name, integer_to_list(N)),
                   {ok, Worker} = monic_worker:start_link(self(), Path),
                   Worker
               end || N <- lists:seq(1, Concurrency)],
    #state{idle=Workers}.

shutdown_workers(#state{busy=Busy,idle=Idle}) ->
    lists:foreach(fun(Worker) -> monic_worker:close(Worker) end,
                  Busy ++ Idle),
    ok.
