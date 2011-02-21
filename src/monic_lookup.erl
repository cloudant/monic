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

-module(monic_lookup).
-behaviour(gen_server).

-export([start_link/0, lookup/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

lookup(Path, Key) ->
    case gen_server:call(?MODULE, {lookup, Key}, []) of
        not_loaded ->
            {ok, Tid} = monic_file:load(Path),
            lookup(Path, Key);
        Else ->
            Else
    end.

%% gen_server functions.

init(_) ->
    {ok, dict:new()}.

handle_call({lookup, Path, Key}, _From, Dict) ->
    case dict:find(Path, Dict) of
        {ok, Tid} ->
            case ets:match_object(Tid, {Key, '_', '_'}) of
                [Key, Cookie, Location] ->
                    {reply, {ok, Cookie, Location}, Dict};
                [] ->
                    {reply, not_found, Dict}
            end;
        error ->
            {reply, not_loaded, Dict}
    end.

handle_cast(_Msg, Dict) ->
    {noreply, Dict}.

handle_info(_Info, Dict) ->
    {noreply, Dict}.

terminate(_Reason, Dict) ->
    ok.

code_change(_OldVsn, Dict, _Extra) ->
  {ok, Dict}.

%% private functions
