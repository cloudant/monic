%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Callbacks for the monic application.

-module(monic_app).
-author('author <author@example.com>').

-behaviour(application).
-export([start/2,stop/1]).


%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for monic.
start(_Type, _StartArgs) ->
    monic_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for monic.
stop(_State) ->
    ok.
