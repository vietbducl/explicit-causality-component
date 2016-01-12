-module(ecc_core).

-behaviour(gen_server).

-export([
         start_link/0,
         put/3,
         put_simple/3,
         put_remote/4,
         get/1,
         delete/1,
         delete_all_keys/0,
         get_key_version/1,
         get_buffer_list/0,
         list_keys/0
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%-define(SERVER, ?MODULE).

-define(DATA, data).


%% storepid: pid of riak store,
%% clientpid: pid of ecc
-record(state, {storepid, clientpid, neighborlist, buffer}).

%%% ======== API ========= %%%
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

put(Key, Value, Dependencies) -> 
    gen_server:call(?MODULE, {put, Key, Value, Dependencies} ).

put_remote(Key, Version, Value, Dependencies) -> 
    gen_server:call(?MODULE, {putremote, Key, Version, Value, Dependencies} ).

put_simple(Key, Value, Dependencies) -> 
    gen_server:call(?MODULE, {putsimple, Key, Value, Dependencies} ).

get(Key) -> 
    gen_server:call(?MODULE, {get, Key} ).

delete(Key) -> 
    gen_server:call(?MODULE, {delete, Key} ).

list_keys() -> 
    gen_server:call(?MODULE, {listkeys} ).

delete_all_keys() -> 
    gen_server:call(?MODULE, {deleteallkeys} ).

get_key_version(Key) -> 
    gen_server:call(?MODULE, {getkeyversion, Key} ).

get_buffer_list() -> 
    gen_server:call(?MODULE, {getbufferlist} ).


%%% ======== CALLBACKS ========= %%%
init([]) ->
    %RiakPort = 10037,
    RiakPort = ecc:get_riakport(),

    {ok, StorePid} = ecc_store:init(RiakPort),
    out("connected to riakc socket with storepid: ~p", [StorePid]),

    NeighborList = ecc:list_neighbor(),
    ecc:ensure_contact(NeighborList),

    %%Start own Client
    {ok, ClientPid} = ecc:start_link(),

    %%initialize state record 
    State = #state{storepid = StorePid, clientpid = ClientPid, 
                    neighborlist = NeighborList, buffer = []},

    out("Core state record: StorePid: ~p ClientPid: ~p~n NeighborList: ~p",
        [State#state.storepid, State#state.clientpid, State#state.neighborlist]),    
    {ok, State}.  

handle_call({get, Key}, _From, State) ->
    %out("EccCore: receive get/key call"),
    StorePid = State#state.storepid,
    Res = get_data_from_riak(StorePid, Key),
    {reply, Res, State};

handle_call({put, Key, Value, Dependencies}, _From, State) ->
    %out("EccCore: receive put/key/value call"),
    StorePid = State#state.storepid,
    NeighborList = State#state.neighborlist,
    Res = put_data_to_riak(StorePid, Key, Value),
    {ok, _, NewVersion} = Res,
    Update = {Key, NewVersion, Value, Dependencies},
    %file:write_file("overhead.txt", io_lib:fwrite("~p.\n",[length(Dependencies)])),
    %out("~p", [length(Dependencies)]),
    lists:foreach(
        fun(Neighbor) ->
            % out("EccCore: send cast to other"),
            gen_server:cast({?MODULE, Neighbor}, {update, {node(), Update}})
        end,
        NeighborList),
    %out("~p", [Res]),
    {reply, Res, State};


%put simple: send update simple to neighbor
handle_call({putsimple, Key, Value, Dependencies}, _From, State) ->
    %out("EccCore: receive put simple /key/value call"),
    StorePid = State#state.storepid,
    NeighborList = State#state.neighborlist,
    Res = put_data_to_riak(StorePid, Key, Value),
    {ok, _, NewVersion} = Res,
    Update = {Key, NewVersion, Value, Dependencies},
    lists:foreach(
        fun(Neighbor) ->
             %out("EccCore: send cast to other"),
            gen_server:cast({?MODULE, Neighbor}, {updatesimple, {node(), Update}})
        end,
        NeighborList),
    out("~p", [Res]),
    {reply, Res, State};

handle_call({delete, Key}, _From, State) ->
    out("EccCore: receive delete/key call"),
    StorePid = State#state.storepid,
    Res = delete_data_from_riak(StorePid, Key),
    {reply, Res, State};

handle_call({deleteallkeys}, _From, State) ->
    out("EccCore: receive delete all keys call"),
    StorePid = State#state.storepid,
    Res = delete_all_keys(StorePid),
    {reply, Res, State};

handle_call({getbufferlist}, _From, State) -> 
    Res = State#state.buffer,
    {reply, Res, State};

handle_call({getkeyversion, Key}, _From, State) ->
    StorePid = State#state.storepid,
    Res = get_version_of_key(StorePid, Key), 
    {reply, Res, State};

handle_call({listkeys}, _From, State) ->
    %out("EccCore: receive listkeys call"),
    StorePid = State#state.storepid,
    Res = list_keys_from_riak(StorePid, ?DATA),
    {reply, Res, State}.

handle_cast({update, {_, Update}}, State) ->
    %out("Self: ~p reveived update: ~p from ~p", [node(), Update, Sender]),
    StorePid = State#state.storepid,
    {Key, Version, Value, Dependencies} = Update,
    case is_newer_version(StorePid, Key, Version) of 
        true -> 
           % out("Update has new version of key ~p", [Key]), 
            case is_depencency_satisfied(StorePid, Dependencies) of 
                true -> 
                    %out("Dependency check: succeeded"),
                    Res = do_put_data_to_riak(StorePid, Key, Version, Value);
                {false, X} -> 
                    %out("Dependency check: not succeed"),
                    Res = {error, X};
                _ -> 
                    Res = {error}

            end; 
        _ -> 
            %out("Old version of key, discard"),
            Res = {error, oldversion, discard}
    end,

    case Res of 
        {error, L} -> 
            % L = unsatisfied dependencies
            BufferList = State#state.buffer, 
            %out("old buffer ~p", [BufferList]),
            NewBufferList = [{Key, Version, Value, L} | BufferList],
             %out("receive depencency unsatisfied. Put in buffer: ~p", [{Key, Version, Value, L}] ),
            NewState = State#state{buffer = NewBufferList};
        {ok, _, _} -> 
            BufferList = State#state.buffer, 
            %out("refresh_buffer: ~p", [BufferList]),
            %% By compare the most recent put_remote {Key, Version} with BufferList to check
            %% if it satisfy any dependencies             
            NewBufferList = lists:filtermap(fun(X) -> 
                case refresh_buffer(StorePid, Key, Version, X) of
                    false -> false;
                    {true, NewBuffer} -> {true, NewBuffer}
                end
            end, BufferList),


            NewState = State#state{buffer = NewBufferList};
            %out("BufferList remain: ~p", [NewState#state.buffer]);
        _ -> 
            %out("recheck buffer if any statisfied after new put"),
            NewState = State
    end,
    %out("Buffer of New state:  ~p ", [NewState#state.buffer]),
    {noreply, NewState};

%update simple: put in without dependency check, dont care buffer
handle_cast({updatesimple, {_, Update}}, State) ->
    %out("Self: ~p reveived update simple: ~p from ~p", [node(), Update, Sender]),
    StorePid = State#state.storepid,
    {Key, Version, Value, _} = Update,
    case is_newer_version(StorePid, Key, Version) of 
        true -> 
            do_put_data_to_riak(StorePid, Key, Version, Value);
        _ -> 
            out("Old version of key, discard")
            %{error, oldversion, discard}
    end,

    {noreply, State};

handle_cast({ok, []}, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    {stop, normal, State}.

terminate(_Reason, _State) ->
    {ok, _State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% ======== INTERNAl FUNCTIONS ========= %%%

get_data_from_riak(StorePid, Key) -> 
    ecc_store:lookup(StorePid, ?DATA, Key).

put_data_to_riak(StorePid, Key, NewValue ) -> 
    Version = get_version_of_key(StorePid, Key),
    NewVersion = Version + 1,
    do_put_data_to_riak(StorePid, Key, NewVersion, NewValue).

do_put_data_to_riak(StorePid, Key, Version, NewValue) -> 
    Res = ecc_store:insert(StorePid, ?DATA, Key, Version, NewValue),
    case Res of 
        ok -> 
            {ok, Key, Version};
        {ok, _} -> 
            {ok, Key, Version};
        _ -> 
            {error}
    end.  

delete_data_from_riak(StorePid, Key) -> 
    ecc_store:delete(StorePid, ?DATA, Key).


list_keys_from_riak(StorePid, Bucket) -> 
    ecc_store:list(StorePid, Bucket).

delete_all_keys(StorePid) -> 
    KeyList = list_keys_from_riak(StorePid, ?DATA),
    DeletedListRes = lists:map(fun(Key) -> 
        {TheKey} = Key,
        delete_data_from_riak(StorePid, TheKey)
        end, KeyList),
    DeletedListRes.

get_version_of_key(StorePid, Key) -> 
    ecc_store:version_of_key(StorePid, ?DATA, Key).

is_newer_version(StorePid, Key, Version) -> 

    RiakVersion = get_version_of_key(StorePid, Key),
    %out("riak version ~p newversion ~p", [RiakVersion, Version]),
    if 
        Version > RiakVersion -> true;
        true -> false
    end. 

%% foreach key in dep_lists that satisfied, drop it. If all keys satisfied, L return [] 
%% in case of too many keys, will change to foreach each and break immediately when 
%% a key that not satisfied is found
is_depencency_satisfied(StorePid, Dependencies) -> 
    %out("Start checking if each depencency satisfied"),
    L = lists:dropwhile(fun(X) -> check_each_dependency(StorePid, X) end, Dependencies),
    if 
        length(L) =:= 0 -> 
            %out("Summary: all dependencies satisfied"),
            true; 
        true -> 
            %out("Summary: some dependencies not satisfied"),
            {false, L}
    end. 

check_each_dependency(StorePid, Depencency) -> 
    {K, V} = Depencency, 
    %out("Start checking depencency key satisfied: ~p", [K]),   
    RiakVersion = get_version_of_key(StorePid, K),
    if
        V =< RiakVersion -> 
            %out("version for depencency key: ~p satisfied", [K]),
            true;
        RiakVersion < 0 -> 
            %out("Riak has not key: ~p yet, not satisfied", [K]),  
            false;
        true -> 
            %out("version for depencency key: ~p not satisfied", [K]),
            false
    end.

refresh_buffer(StorePid, RecentKey, RecentVersion, Buffer) -> 
    {Key, Value, Version, DependencyRemains} = Buffer,
    NewDependencyRemains = lists:dropwhile(fun(X) -> recheck_dependency_buffer(RecentKey, RecentVersion, X) end, DependencyRemains),
    if 
        length(NewDependencyRemains) =:= 0 -> 
            %out("Recheck buffer: all dependencies of update {~p, ~p} satisfied. Put to Riak.", [Key, Version]),
            do_put_data_to_riak(StorePid, Key, Version, Value),
            false;
        true -> 
            %out("Summary: some dependencies not satisfied"),
            NewBuffer = {Key, Version, Value, NewDependencyRemains},
            {true, NewBuffer}
    end.  

recheck_dependency_buffer(RecentKey, RecentVersion, Dependency) -> 
    {Key, Version} = Dependency,
    case RecentKey of 
        Key ->
            if RecentVersion < Version -> false;
            true -> true
            end;
        _ -> 
            false 

    end.

%%% ======== HELPER METHODS ========= %%%
out(Format) ->
  out(Format, []).
out(Format, Data) ->
  io:format("~p:~p: ~s~n", [?MODULE, self(), io_lib:format(Format, Data)]).
