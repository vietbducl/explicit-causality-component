-module(ecc_java).

-export([insert/3, insert_cops/3, insert_simple/3,
		lookup/1, lookup_simple/1,
		delete/1, list_keys/0]).

insert_cops(Key, _, Value) ->
	ecc:insert_auto_dep(Key, Value),    
    %out("insert in cops"),
    ok.

insert(Key, ParentKey, Value) -> 
	case ParentKey of 
		'-1' -> 
			ecc:insert(Key, Value, []);
		_ -> 
			ecc:insert(Key, Value, [{ParentKey, 0}])
	end,
	ok.

insert_simple(Key, _, Value) -> 
	ecc:insert_simple(Key, Value),
	ok.

lookup_simple(Key) -> 
	ecc:lookup_simple(Key).

lookup(Key) ->
	ecc:lookup(Key).
	%ecc:lookup_simple(Key).

delete(Key) ->
    ecc_core:delete(Key).

list_keys() -> 
    ecc_core:list_keys().

%%% ======== HELPER METHODS ========= %%%
out(Format) ->
  out(Format, []).
out(Format, Data) ->
  io:format("~p:~p: ~s~n", [?MODULE, self(), io_lib:format(Format, Data)]).