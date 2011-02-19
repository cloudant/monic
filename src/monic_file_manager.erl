%% @author author <author@example.com>
%% @copyright YYYY author.
%% @doc Create and delete monic files.

-module(monic_file_manager).
-export([init/1,
    allowed_methods/2,
    content_types_accepted/2,
    content_types_provided/2,
    delete_resource/2,
    is_conflict/2,
    resource_exists/2]).
    
-export([from_json/2, to_json/2]).

-include_lib("webmachine/include/webmachine.hrl").

allowed_methods(ReqData, Context) ->
    {['DELETE', 'GET', 'PUT'], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
    {[{"application/json", from_json}], ReqData, Context}.
    
content_types_provided(ReqData, Context) ->
    {[{"application/json", to_json}], ReqData, Context}.
    
delete_resource(ReqData, Context) ->
    case file:delete(path(ReqData)) of
        ok ->
            {true, ReqData, Context};
        _ ->
            {false, ReqData, Context}
    end.

init([]) ->
    {ok, undefined}.

is_conflict(ReqData, Context) ->  
    {exists(ReqData), ReqData, Context}.

resource_exists(ReqData, Context) ->    
    {exists(ReqData), ReqData, Context}.

from_json(ReqData, Context) ->
    ok = file:write_file(path(ReqData), <<>>, [exclusive]),
    {true, ReqData, Context}.
    
to_json(ReqData, Context) ->
    {"{\"ok\": true}", ReqData, Context}.

path(ReqData) ->
    wrq:path_info(file, ReqData).

exists(ReqData) ->
    filelib:is_file(path(ReqData)).

