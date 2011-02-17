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

-module(monic_fake_stream).
-behaviour(gen_server).

%% public API
-export([start_link/1, next/1, close/1]).

%% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

start_link(Stream) ->
    gen_server:start_link(?MODULE, Stream, []).

next(Pid) ->
    gen_server:call(Pid, next).

close(Pid) ->
    gen_server:call(Pid, close).

init(Stream) ->
    {ok, Stream}.

handle_call(next, _From, [T]=Stream) ->
    {reply, T, Stream};
handle_call(next, _From, [H|T]) ->
    {reply, H, T};
handle_call(close, _From, Stream) ->
    {stop, normal, ok, Stream}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

