-module(monic).
-behavior(gen_server).

% public API
-export([start_link/1, start_link/2, close/1, write/2, read/2]).

% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
          busy=[],
          idle=[],
          requests=[]}).

% public functions

start_link(Name) ->
    start_link(Name, []).

start_link(Name, Options) ->
    gen_server:start_link({local, list_to_atom(Name)}, ?MODULE, [Name, Options], []).

close(Pid) ->
    gen_server:call(Pid, close, infinity).

write(Pid, Bin) when is_binary(Bin) ->
    gen_server:call(Pid, {write, Bin}, infinity).

read(Pid, Handle) ->
    gen_server:call(Pid, {read, Handle}, infinity).

% gen_server functions

init([Name, Options]) ->
    State = init_workers(Name, Options),
    {ok, State}.

handle_call(close, _From, State) ->
    shutdown_workers(State),
    {stop, normal, ok, State};
handle_call(Request, From, #state{idle=[W|Rest]}=State) ->
    monic_worker:start_work(W, {Request, From}),
    {noreply, State#state{idle=Rest,busy=[W | State#state.busy]}};
handle_call(Request, From, #state{idle=[]}=State) ->
    {noreply, State#state{requests=[{Request, From} | State#state.requests]}}.

handle_cast({done, Worker, From, Resp}, #state{requests=[]}=State) ->
    Busy = [B || B <- State#state.busy, B /= Worker],
    Idle = [Worker | State#state.idle],
    gen_server:reply(From, Resp),
    {noreply, State#state{idle=lists:reverse(Idle), busy=Busy}};
handle_cast({done, Worker, From, Resp}, #state{requests=[R|Rest]}=State) ->
    gen_server:reply(From, Resp),
    monic_worker:start_work(Worker, R),
    {noreply, State#state{requests=Rest}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    shutdown_workers(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% private functions

init_workers(Name, Options) ->
    Max = get_value(max, Options, 1),
    Workers = lists:map(fun(N) ->
                                Path = filename:join(Name, integer_to_list(N)),
                                {ok, Worker} = monic_worker:start_link(self(), Path),
                                Worker end, lists:seq(1, Max)),
    #state{idle=Workers}.

shutdown_workers(#state{busy=Busy,idle=Idle}) ->
    lists:foreach(fun(Worker) -> monic_worker:close(Worker) end,
                  Busy ++ Idle),
    ok.

get_value(Key, Props, Default) ->
    case lists:keyfind(Key, 1, Props) of
        false -> Default;
        {_, Value} -> Value
    end.
