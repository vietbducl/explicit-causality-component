-module(ecc).
-behaviour(gen_server).

-export([start_link/0, 
			insert/3, 
			insert_auto_dep/2,
			insert_simple/2,
			lookup/1, 
			lookup_simple/1,
			delete/1, 
			list_keys/0,
			delete_all_keys/0,
			list_neighbor/0,
			ensure_contact/1,
			get_riakport/0,
			get_key_version/1,
			get_deplist/0,
			get_parentCount/0,
			get_buffer_list/0
		]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {dependencies, parentcount}).

start_link() -> 
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

insert(Key, Value, Dependencies) -> 
	gen_server:call(?MODULE, {put, Key, Value, Dependencies}).

insert_auto_dep(Key, Value) -> 
	gen_server:call(?MODULE, {putautodep, Key, Value}).

insert_simple(Key, Value) ->
	gen_server:call(?MODULE, {putsimple, Key, Value}).

lookup(Key) ->
	gen_server:call(?MODULE, {get, Key}).

lookup_simple(Key) -> 
	gen_server:call(?MODULE, {getsimple, Key}).

delete(Key) ->
	gen_server:call(?MODULE, {delete, Key}).

list_keys() -> 
	gen_server:call(?MODULE, {list_keys}).

delete_all_keys() -> 
	gen_server:call(?MODULE, {deleteallkeys}).

get_deplist() -> 
	gen_server:call(?MODULE, {getdeplist}).

get_key_version(Key) -> 
    gen_server:call(?MODULE, {getkeyversion, Key}).

get_buffer_list() -> 
	gen_server:call(?MODULE, {getbufferlist}).


get_parentCount() -> 
	gen_server:call(?MODULE, {getparentcount}).

%%% ======== CALLBACKS ========= %%%
init([]) ->    
	Dependencies = [],
	ParentCount = 0,
	State = #state{dependencies = Dependencies, parentcount = ParentCount},
	out("ECC state record: Dependencies: ~p ParentCount: ~p~n ",
        [State#state.dependencies, State#state.parentcount]), 
    {ok, State}.  

handle_call({put, Key, Value, Dependencies}, _From, State) ->
	%Dependencies = State#state.dependencies,	
	Res = ecc_core:put(Key, Value, Dependencies),
    {reply, Res, State};

%put auto dep: for testing with YCSB, auto add dependency from client library
handle_call({putautodep, Key, Value}, _From, State) ->
	out("insert autodep for cops"),
	Dependencies = State#state.dependencies,	
	Res = ecc_core:put(Key, Value, Dependencies),
	ParentCount = State#state.parentcount,
	L = length(Dependencies),
	NewParentCount = ParentCount + L,
	%out("New PC: ~p", [NewParentCount]),
	out("insert autodep for cops with parentcount ~p", [NewParentCount]),
	%NewParentCount = 0,
	case Res of 
        {ok, K, Ver} -> 
            %out("repopulate client library dependency list"),
            NewDep = [{K, Ver}],
            NewState = #state{dependencies = NewDep, parentcount = NewParentCount};
        _ -> 
            NewState = State#state{parentcount = NewParentCount}
            %out("put fail")
    end,
    {reply, Res, NewState};

%put simple: 
handle_call({putsimple, Key, Value}, _From, State) ->
	%Dependencies = State#state.dependencies,	
	Res = ecc_core:put_simple(Key, Value, []),
    {reply, Res, State};

handle_call({delete, Key}, _From, State) ->
	Res = ecc_core:delete(Key),
    {reply, Res, State};

handle_call({list_keys}, _From, State) ->
	Res = ecc_core:list_keys(),
    {reply, Res, State};

handle_call({deleteallkeys}, _From, State) ->
	Res = ecc_core:delete_all_keys(),
    {reply, Res, State};

handle_call({getparentcount}, _From, State) ->

	ParentCount = State#state.parentcount,
	%out("~p", ParentCount),
    {reply, ParentCount, State};

handle_call({getbufferlist}, _From, State) -> 
	Res = ecc_core:get_buffer_list(),
	{reply, Res, State};

handle_call({getkeyversion, Key}, _From, State) ->
	Res = ecc_core:get_key_version(Key),
    {reply, Res, State};    

handle_call({getdeplist}, _From, State) ->
	Dependencies = State#state.dependencies,
    out("current dependencies: "),
    lists:map(fun({X, Y}) -> out("{~p,~p}", [X,Y]) end, Dependencies),
    %another way
    %lists:flatten(io_lib:format("~p", [Dependencies])),
    Res = ok,
    {reply, Res, State};

handle_call({getsimple, Key}, _From, State) ->	
	Res = ecc_core:get(Key),
    {reply, Res, State};

handle_call({get, Key}, _From, State) ->	
	Dependencies = State#state.dependencies,
	Res = ecc_core:get(Key),
	out("read "),
	case Res of 
		{ok, K, Ver, _} -> 
			%out("add get to client library dependency list"),
			NewDep = [{K, Ver} | Dependencies],
			NewState = State#state{dependencies = NewDep};
		_ -> 
			NewState = State
			%out("get fail")
		end,
    {reply, Res, NewState}.

handle_cast({ok, []}, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    {stop, normal, State}.

terminate(_Reason, _State) ->
    {ok, _State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%% ======== ENV ========= %%%
get_env(AppName, Key, Default) ->
    case application:get_env(AppName, Key) of
        undefined   -> Default;
        {ok, Value} -> Value
    end.

list_neighbor() -> 
	DefaultNodes = [n1@localhost, n2@localhost],
	case get_env(ecc, neighbors, DefaultNodes) of
		[] -> 
			io:format("error, find none");
		Value ->
			%subtract self to get list of neighbor
			NeighborList = lists:subtract(Value, [node()]), 
			NeighborList
	end.

ensure_contact(NeighborList) ->
	Answering = [N || N <- NeighborList, net_adm:ping(N) =:= pong],
   case Answering of
    [] ->
        {error, no_contact_nodes_reachable};
    _ ->
    	io:format("net_adm: ok"),
    	ok
   end.

get_riakport() -> 
	DefaultPort = 10017,
	RiakPort =  get_env(ecc, riakport, DefaultPort),
	RiakPort.

%%% ======== HELPER METHODS ========= %%%
out(Format) ->
  out(Format, []).
out(Format, Data) ->
  io:format("~p:~p: ~s~n", [?MODULE, self(), io_lib:format(Format, Data)]).