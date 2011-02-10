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

-module(monic_test).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    {ok, Pid} = monic:start_link("foo", [{max, 5}]),
    Bin = <<"hello this is a quick test">>,
    {ok, Handle} = monic:write(Pid, Bin),
    {ok, Bin1} = monic:read(Pid, Handle),
    ?assertEqual(Bin, Bin1),
    ok = monic:close(Pid).
