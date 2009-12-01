-module(kvs).
-export([get/1, get/2, set/2, set/3, start/1, stop/0, store/1]).
-include("kvs.hrl").

%% @doc create N nodes in distributed key-value store
%% @spec start(integer()) -> started
start(N) ->
    pg2:create(kvs),
    lists:foreach(fun(_) ->
			  Store = #kvs_store{data=[],
					     pending_reads=[],
					     pending_writes=[]},
			  pg2:join(kvs, spawn(kvs, store, [Store]))
		  end, lists:seq(0, N)),
    started.

%% @doc stop all pids in KVS process group
%% stop() -> stopped.
stop() ->
    lists:foreach(fun(Pid) ->
			  pg2:leave(kvs, Pid),
			  Pid ! stop
		  end, pg2:get_members(kvs)),
    stopped.

%% @doc retrieve value for key
%% @spec get(term()) -> value() | undefined
%%       value = term()
get(Key) -> get(Key, ?TIMEOUT).

%% @doc retrieve value for key, with timeout
%% @spec get(term(), integer()) -> val() | timeout()
%%       val = {ok, term()} | {ok, undefined}
%%       timeout = {error, timeout}
get(Key, Timeout) ->
    Pid = pg2:get_closest_pid(kvs),
    Pid ! {self(), get, Key},
    receive
	{Pid, got, Value} ->
	    {ok, Value}
    after 
	Timeout ->
	    {error, timeout}
    end.

%% @doc update value for key
%% @spec set(term(), term()) -> {ok, updated} | {error, timeout}
set(Key, Val) -> set(Key, Val, ?TIMEOUT).

%% @doc update value for key, with timeout
%% @spec set(term(), term()) -> {ok, updated} | {error, timeout}
set(Key, Val, Timeout) ->
    Pid = pg2:get_closest_pid(kvs),
    Pid ! {self(), set, Key, Val},
    receive
	{Pid, received, {set, Key, Val}} ->
	    {ok, updated}
    after
	Timeout ->
	    {error, timeout}
    end.

%% @doc implementation of distributed key-value store
%% @spec store(proplist()) -> term()
%%       proplist = [{term(), term()}]
store(Store = #kvs_store{data=Data, pending_reads=Reads, pending_writes=Writes}) ->
    receive 
	{Sender, get, Key} ->
	    % client interface for retrieving values
	    Sender ! {self(), got, proplists:get_value(Key, Data)},
	    store(Store);
	{Sender, set, Key, Value} ->
	    % client interface for updating values
	    lists:foreach(fun(Pid) ->
				  Pid ! {self(), update, Sender, Key, Value}
			  end, pg2:get_members(kvs)),
	    Writes2 = [{{Sender, Key}, ?KVS_WRITES} | Writes],
	    store(Store#kvs_store{pending_writes=Writes2});
	{Sender, update, Client, Key, Value} ->
	    % sent to all nodes by first receiving node
	    Sender ! {self(), updated, Client, Key, Value},
	    store(Store#kvs_store{data=[{Key, Value} | proplists:delete(Key, Data)]});
	{_Sender, updated, Client, Key, Value} ->
	    Count = proplists:get_value({Client, Key}, Writes),
	    case Count of
		undefined ->
		    store(Store);
		1 ->
		    Client ! {self(), received, {set, Key, Value}},
		    store(Store#kvs_store{pending_writes=proplists:delete({Key, Value}, Writes)});
		_ ->
		    store(Store#kvs_store{pending_writes=[{{Client, Key}, Count-1}, proplists:delete({Client, Key}, Writes)]})
	    end;
	stop ->
	    ok
    end.

