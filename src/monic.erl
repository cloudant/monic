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
-export([write/2, read/2, read/3, delete/2]).
-include("monic.hrl").

start() ->
    application:start(monic).

stop() ->
    application:stop(monic).

write(Group, Bin) when is_binary(Bin) ->
    gen_server:call(group(Group), {write, Bin}, infinity);
write(Group, Fun) when is_function(Fun) -> % TODO
    gen_server:call(group(Group), {write, Fun}, infinity).

read(Group, #handle{}=Handle) ->
    gen_server:call(group(Group), {read, Handle}, infinity).

read(Group, #handle{}=Handle, Fun) when is_function(Fun) -> % TODO
    gen_server:call(group(Group), {read, Handle, Fun}, infinity).

delete(Group, #handle{}=Handle) ->
    gen_server:call(group(Group), {delete, Handle}, infinity).

group(Group) ->
    Spec = {
      Group,
      {gen_server, start_link, [monic_group, [Group], []]},
      temporary, 1, worker, [?MODULE]
     },
    case supervisor:start_child(monic_sup, Spec) of
        {ok, Pid} ->
            Pid;
        {error, {already_started, Pid}} ->
            Pid
    end.
