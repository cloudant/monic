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

-module(monic).
-export([start/0, stop/0]).
-export([write/2, read/2, read/3, read/4, delete/2]).
-include("monic.hrl").

start() ->
    application:start(monic).

stop() ->
    application:stop(monic).

write(Group, Bin) when is_binary(Bin) ->
    call(Group, {write, Bin});
write(Group, Fun) when is_function(Fun) ->
    call(Group, {write, Fun}).

read(Group, #handle{}=Handle) ->
    call(Group, {read, Handle}).

read(Group, #handle{}=Handle, Fun) when is_function(Fun) ->
    call(Group, {read, Handle, Fun}).

read(Group, #handle{}=Handle, Ranges, Fun) when is_function(Fun) ->
    call(Group, {read, Handle, Ranges, Fun}).

delete(Group, #handle{}=Handle) ->
    call(Group, {delete, Handle}).

call(Group, Msg) ->
    gen_server:call(group(Group), Msg, infinity).

group(Group) ->
    Spec = {
      Group,
      {gen_server, start_link, [monic_group, [Group], []]},
      temporary, 1, worker, [?MODULE]
     },
    %% relevant parts of couch_rep.erl below.
    case supervisor:start_child(monic_sup, Spec) of
        {ok, Pid} ->
            Pid;
        {error, {already_started, Pid}} ->
            Pid;
        {error, already_present} ->
            case supervisor:restart_child(monic_sup, Group) of
                {ok, Pid} ->
                    Pid;
                {error, running} ->
                    {error, {already_started, Pid}} =
                        supervisor:start_child(monic_sup, Spec),
                    Pid
            end
    end.
