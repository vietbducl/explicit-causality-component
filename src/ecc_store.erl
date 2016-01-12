-module(ecc_store).

-export([
         init/1,
         lookup/3,
         insert/5,
         delete/3,
         version_of_key/3,
         list/2
        ]).


init(RiakPort) ->
	{ok, _} = riakc_pb_socket:start_link("127.0.0.1", RiakPort).

lookup(StorePid, Bucket, Key) -> 
    %out("ecc_store: lookup key from riak"),
    BinBucket = term_to_binary({Bucket}),
    BinKey = term_to_binary({Key}),
    case riakc_pb_socket:get(StorePid, BinBucket, BinKey) of
        {ok, FetchDatas} -> 
            {Version, Value} = binary_to_term(riakc_obj:get_value(FetchDatas)),
             {ok, Key, Version, Value};
        {error, notfound} -> 
            {ok, notfound}
    end. 

insert(StorePid, Bucket, Key, Version, NewValue ) -> 
	%out("ecc_store: insert key to riak"),
    BinBucket = term_to_binary({Bucket}),
    BinKey = term_to_binary({Key}),
    VerVal = {Version, NewValue},
    Object = riakc_obj:new(BinBucket, BinKey, VerVal),
    Res = riakc_pb_socket:put(StorePid, Object),  
    out(Res).

delete(StorePid, Bucket, Key) -> 
    out("ecc_store: in delete data with key ~p in buckets", [Key]),
    BinBucketData = term_to_binary({Bucket}),
    BinKey = term_to_binary({Key}),
    riakc_pb_socket:delete(StorePid, BinBucketData, BinKey).

list(StorePid, Bucket) -> 
    BinBucket = term_to_binary({Bucket}),
    Res = riakc_pb_socket:list_keys(StorePid, BinBucket),
    {ok, BinKeyList} = Res,
    KeyList = lists:map(fun(X) -> binary_to_term(X) end, BinKeyList),
    KeyList.

version_of_key(StorePid, Bucket, Key) -> 
    BinBucket = term_to_binary({Bucket}),
    BinKey = term_to_binary({Key}),
    case riakc_pb_socket:get(StorePid, BinBucket, BinKey) of
        {ok, FetchDatas} -> 
            {Version, _} = binary_to_term(riakc_obj:get_value(FetchDatas)),
            Version;
        {error, notfound} -> 
            -1     
    end. 


%%% ======== HELPER METHODS ========= %%%
out(Format) ->
  out(Format, []).
out(Format, Data) ->
  io:format("~p:~p: ~s~n", [?MODULE, self(), io_lib:format(Format, Data)]).