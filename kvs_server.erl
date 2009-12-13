%% @doc kvs_server implements a distributed key-value store.
-module(kvs_server).
-include("kvs.hrl").
-export([store/1]).

%% @doc implementation of distributed key-value store
%% @spec store(proplist()) -> term()
%%       proplist = [{term(), term()}]
store(Store) ->
    receive 
	{Sender, get, Key} ->
	    message_get(Store, Sender, Key);
	{Sender, retrieve, Client, Key} ->
	    message_retrieve(Store, Sender, Client, Key);
	{Sender, retrieved, Client, Key, Value} ->
	    message_retrieved(Store, Sender, Client, Key, Value);
	{Sender, set, Key, Value} ->
	    message_set(Store, Sender, Key, Value);
	{Sender, update, Client, Key, Value} ->
	    message_update(Store, Sender, Client, Key, Value);
	{Sender, updated, Client, Key, Value} ->
	    message_updated(Store, Sender, Client, Key, Value);
	stop ->
	    ok
    after
	?KVS_POLL_PENDING ->
	    Writes2 = lists:filter(fun filter_writes/1, Store#kvs_store.pending_writes),
	    Reads2 = lists:filter(fun filter_reads/1, Store#kvs_store.pending_reads),
	    store(Store#kvs_store{pending_writes=Writes2, pending_reads=Reads2})
    end.

%% @doc filter expired writes from pending_writes.
filter_writes({{Client, _Key}, {_Count, Ts}}) ->
    Now = ts(),
    if Now > Ts + ?KVS_WRITE_TIMEOUT ->
	    Client ! {error, write_failed},
	    false;
       true  ->
	    true
    end.

%% @doc filter expired reads from pending_reads.
filter_reads({{Client, _Key}, {_Count, _Values, Ts}}) ->
    Now = ts(),
    if Now > Ts + ?KVS_READ_TIMEOUT ->
	    Client ! {error, read_failed},
	    false;
       true  ->
	    true
    end.

%%
%% Message Handling Functions
%%

message_get(Store = #kvs_store{pending_reads=Reads}, Sender, Key) ->
    % client interface for retrieving values
    lists:foreach(fun(Pid) ->
			  Pid ! {self(), retrieve, Sender, Key}
		  end, pg2:get_members(kvs)),
    % ?KVS_READS is required # of nodes to read from
    % [] is used to collect read values
    Reads2 = [{{Sender, Key}, {?KVS_READS, [], ts()}} | Reads],
    store(Store#kvs_store{pending_reads=Reads2}).

message_retrieve(Store = #kvs_store{data=Data}, Sender, Client, Key) ->
    Sender ! {self(), retrieved, Client, Key, proplists:get_value(Key, Data)},
    store(Store).

message_retrieved(Store = #kvs_store{pending_reads=Reads}, _Sender, Client, Key, Value) ->
    case proplists:get_value({Client, Key}, Reads) of
	undefined ->
	    store(Store);
	{0, Values, _Timestamp} ->
	    Freq = lists:foldr(fun(X, Acc) ->
				       case proplists:get_value(X, Acc) of
					   undefined -> [{X, 1} | Acc];
					   N -> [{X, N+1} | proplists:delete(X, Acc)]
				       end
			       end, [], Values),
	    [{Popular, _} | _ ] = lists:reverse(lists:keysort(2, Freq)),
	    Client ! {self(), got, Popular},
	    store(Store#kvs_store{
		    pending_reads=proplists:delete({Key, Value}, Reads)});
	{Count, Values, Timestamp} ->
	    store(Store#kvs_store{
		    pending_reads=[{{Client, Key}, {Count-1, [Value | Values], Timestamp}} |
				   proplists:delete({Client, Key}, Reads)]})
    end.

message_set(Store = #kvs_store{pending_writes=Writes}, Sender, Key, Value) ->
    % client interface for updating values
    lists:foreach(fun(Pid) ->
			  Pid ! {self(), update, Sender, Key, Value}
		  end, pg2:get_members(kvs)),
    Writes2 = [{{Sender, Key}, {?KVS_WRITES, ts()}} | Writes],
    store(Store#kvs_store{pending_writes=Writes2}).


message_update(Store = #kvs_store{data=Data}, Sender, Client, Key, Value) ->    
    % sent to all nodes by first receiving node
    Sender ! {self(), updated, Client, Key, Value},
    store(Store#kvs_store{data=[{Key, Value} | proplists:delete(Key, Data)]}).

message_updated(Store = #kvs_store{pending_writes=Writes}, _Sender, Client, Key, Value) ->
    {Count, Timestamp} = proplists:get_value({Client, Key}, Writes),
    case Count of
	undefined ->
	    store(Store);
	0 ->
	    Client ! {self(), received, {set, Key, Value}},
	    store(Store#kvs_store{
		    pending_writes=proplists:delete({Key, Value}, Writes)});
	_ ->
	    store(Store#kvs_store{
		    pending_writes=[{{Client, Key}, {Count-1, Timestamp}} |
				    proplists:delete({Client, Key}, Writes)]})
    end.

%%
%% Utility functions
%%

ts() ->
    {Mega, Sec, _} = erlang:now(),
    (Mega * 1000000) + Sec.
