# explicit-causality-component
Explicit Causality Component Implementation

To build this code, run the following command:

erlc -o ./ebin ./src/*.erl

To run the program, first start Erlang like this:

erl -pa ./ebin [path-to-riak-erlang-client]/riak-erlang-client/ebin/ ~[path-to-riak-erlang-client]/riak-erlang-client/deps/*/ebin -name [nodename] -setcookie [cookie]

For Example: 

erl -pa ./ebin ~/Documents/Riak/riak-erlang-client/ebin/ ~/Documents/Riak/riak-erlang-client/deps/*/ebin -name n2 -setcookie ecc-ycsb

Then, run the following in the Erlang shell:

1> application:start(ecc).


2> nodes().

//show neighbor

Sample put/get commands:

ecc:insert(one, two, []).

ecc:insert(vanroy, prof, [{manuel, 0}]).

ecc:lookup(one).

Helper commands to check the state of data store: list_keys/0, get_deplist/0, get_buffer_list/0


File ecc_java opens a door for other application to connect to ecc using JInterface. Sample code is provided in java_src. 