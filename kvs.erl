%% @doc kvs is a module which implements the client interface 
%%      for a distributed key-value store.
-module(kvs).
-export([get/1, get/2, set/2, set/3, start/1, stop/0]).
-include("kvs.hrl").

%%%
%%% Client APIs
%%%

%% @doc create N nodes in distributed key-value store
%% @spec start(integer()) -> started
start(N) ->
    pg2:create(kvs),
    lists:foreach(fun(_) ->
			  Store = #kvs_store{data=[],
					     pending_reads=[],
					     pending_writes=[]},
			  pg2:join(kvs, spawn(kvs_server, store, [Store]))
		  end, lists:seq(0, N)),
    started.

%% @doc stop all pids in KVS process group
%% stop() -> stopped.
stop() ->
    LocalFun = fun(X) -> erlang:node(X) == node() end,
    LocalPids = lists:filter(LocalFun, pg2:get_members(kvs)),
    lists:foreach(fun(Pid) ->
			  pg2:leave(kvs, Pid),
			  Pid ! stop
		  end, LocalPids),
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
	    {ok, Value};
	{error, Error} ->
	    {error, Error}
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
	    {ok, updated};
	{error, Error} ->
	    {error, Error}
    after
	Timeout ->
	    {error, timeout}
    end.
