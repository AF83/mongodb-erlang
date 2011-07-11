-module(mongo_id).

-behaviour(gen_server).

-export ([gen_objectid/0, next_requestid/0]).

%% gen_server callbacks
-export([start_link/0, init/1, handle_call/3, handle_cast/2, 
handle_info/2, terminate/2, code_change/3]).

-record(state, {
    oid,
    machineprocid,
    requestid
    }).


%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec next_requestid () -> mongo_protocol:requestid(). % IO
%@doc Fresh request id
next_requestid() -> 
    gen_server:call(?MODULE, counter).

-spec gen_objectid () -> bson:objectid(). % IO
%@doc Fresh object id
gen_objectid() ->
    gen_server:call(?MODULE, gen_objectid).

%%====================================================================
%% api callbacks
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{
        oid = 0,
        machineprocid = machineprocid(),
        requestid = 0
    }}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(counter, _From, #state{oid = Oid} = State) ->
    {reply, Oid+1, State#state{oid = Oid+1}};

handle_call(gen_objectid, _From, #state{oid=Oid, machineprocid=MPid} = State) ->
	Now = bson:unixtime_to_secs(bson:timenow()),
    {reply,
        bson:objectid(Now, MPid, Oid+1),
        State#state{ oid = Oid+1 }
    }; 

handle_call(_Request, _From, State) ->
    {reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Private API
%%--------------------------------------------------------------------

-spec machineprocid () -> <<_:40>>. % IO
%@doc Fetch hostname and os pid and compress into a 5 byte id
machineprocid() ->
	OSPid = list_to_integer (os:getpid()),
	{ok, Hostname} = inet:gethostname(),
	<<MachineId:3/binary, _/binary>> = erlang:md5 (Hostname),
	<<MachineId:3/binary, OSPid:16/big>>.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
    id_test() ->
        start_link(),
        ?assertEqual(1, next_requestid()).
        %TODO : add test for gen_objectid
-endif.
