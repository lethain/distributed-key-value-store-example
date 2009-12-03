-module(kvs_tests).
-include_lib("eunit/include/eunit.hrl").

%% @doc test response when empty kvs process group
not_running_test() ->
    kvs:stop(),
    {error, not_running} = kvs:get(b),
    {error, not_running} = kvs:set(c, 100)    

%% @doc test most basic functionality
basic_test() ->
    started = kvs:start(3),
    {ok, undefined} = kvs:get(a),
    {ok, updated} = kvs:set(a, 5),
    {ok, 5} = kvs:get(a),
    stopped = kvs:stop().

%% @doc test performing many concurrent reads
many_reads_test() ->
    started = kvs:start(3),
    {ok, updated} = kvs:set(b, 100),
    Read = fun() -> {ok, 100} = kvs:get(b) end,
    lists:foreach(fun(_) -> spawn(Read) end,
		  lists:seq(0, 100)),
    stopped = kvs:stop().

%% @doc test performing many concurrent writes
many_writes_test() ->
    started = kvs:start(3),
    Write = fun() -> {ok, updated} = kvs:set(a, 101) end,
    lists:foreach(fun(_) -> spawn(Write) end,
		  lists:seq(0, 100)),
    stopped = kvs:stop().
