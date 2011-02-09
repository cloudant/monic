-module(monic_worker).
-behavior(gen_server).

% public API
-export([start_link/2,start_work/2,close/1]).

% gen_server API
-export([init/1, terminate/2, code_change/3,handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
          name,
          master=nil,
          fd = nil
         }).

-record(handle, {
          name,
          position
         }).

% public functions

start_link(Master, Path) ->
    gen_server:start_link(?MODULE, {Master, Path}, []).

start_work(Pid, Request) ->
    gen_server:cast(Pid, Request).

close(Pid) ->
    gen_server:call(Pid, close, infinity).

% gen_server functions

init({Master, Path}) ->
    filelib:ensure_dir(Path),
    case file:open(Path, [read, write, raw, binary]) of
        {ok, Fd} ->
            {ok, #state{name=filename:basename(Path),master=Master,fd=Fd}};
        Error ->
            Error
    end.

handle_call(close, _From, #state{fd=nil}=State) ->
    {reply, ok, State};
handle_call(close, _From, #state{fd=Fd}=State) ->
    {reply, file:close(Fd), State#state{fd=nil}}.

handle_cast({{read, #handle{name=Name, position=Position}}, From},
            #state{name=Name, master=Master, fd=Fd}=State) ->
    {ok, <<Size:64/integer>>} = file:pread(Fd, Position, 8),
    {ok, Bin} = file:pread(Fd, Position+8, Size),
    gen_server:cast(Master, {done, self(), From, {ok, Bin}}),
    {noreply, State};
handle_cast({{write, Bin}, From},
            #state{name=Name, master=Master, fd=Fd}=State) ->
    Size = byte_size(Bin),
    {ok, Position} = file:position(Fd, cur),
    ok = file:write(Fd, <<Size:64/integer>>),
    ok = file:write(Fd, Bin),
    Handle = #handle{name=Name, position=Position},
    gen_server:cast(Master, {done, self(), From, {ok, Handle}}),
    {noreply, State};
handle_cast({Req, From}, #state{master=Master}=State) ->
    gen_server:cast(Master, {done, self(), From, {error, Req}}),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{fd=nil}) ->
    ok;
terminate(_Reason, #state{fd=Fd}) ->
    file:close(Fd).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
