-module(kvs).
-export([get/1, get/2, set/2, set/3, start/1, stop/0, store/1]).
-define(TIMEOUT, 500).

%% @doc create N nodes in distributed key-value store
%% @spec start(integer()) -> started
start(N) ->
    pg2:create(kvs),
    lists:foreach(fun(_) ->
			  pg2:join(kvs, spawn(kvs, store, [[]]))
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
store(Data) ->
    receive 
	{Sender, get, Key} ->
	    % client interface for retrieving values
	    Sender ! {self(), got, proplists:get_value(Key, Data)},
	    store(Data);
	{Sender, set, Key, Value} ->
	    % client interface for updating values
	    lists:foreach(fun(Pid) ->
				  Pid ! {self(), update, Key, Value}
			  end, pg2:get_members(kvs)),
	    Sender ! {self(), received, {set, Key, Value}},
	    store(Data);
	{Sender, update, Key, Value} ->
	    % sent to all nodes by first receiving node
	    Sender ! {self(), updated, Key, Value},
	    store([{Key, Value} | proplists:delete(Key, Data)]);
	{_Sender, updated, _Key, _Value} ->
	    store(Data);
	stop ->
	    ok
    end.


