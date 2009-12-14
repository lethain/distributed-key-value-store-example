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
    lists:foreach(fun(_) ->
		   kvs_server:start_link()
		  end, lists:seq(0, N)),
    started.

%% @doc stop all pids in KVS process group
%% stop() -> stopped.
stop() ->
    lists:foreach(fun(Pid) ->
			  gen_server:call(Pid, stop)
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
    gen_server:call(Pid, {get, Key}, Timeout).

%% @doc update value for key
%% @spec set(term(), term()) -> {ok, updated} | {error, timeout}
set(Key, Val) -> set(Key, Val, ?TIMEOUT).

%% @doc update value for key, with timeout
%% @spec set(term(), term()) -> {ok, updated} | {error, timeout}
set(Key, Val, Timeout) ->
    Pid = pg2:get_closest_pid(kvs),
    gen_server:call(Pid, {set, Key, Val}, Timeout).
