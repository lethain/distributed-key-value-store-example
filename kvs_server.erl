%% @doc kvs_server implements a distributed key-value store.
-module(kvs_server).
-include("kvs.hrl").
% interface for gen_servers
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% @doc performs initialization of gen_server, return value is the initial state value.
init([]) ->
    process_flag(trap_exit, true),
    pg2:create(kvs),
    pg2:join(kvs, self()),
    {ok, #kvs_store{data=[], pending_reads=[], pending_writes=[]}, ?KVS_POLL_PENDING}.

%% @doc called when gen_server is terminated.
%%      (Note that init function must specify trap_exit
%%       for terminate to be called.)
%% @spec terminate(term(), term()) -> ok.
terminate(_Reason, _State) ->
    pg2:leave(kvs, self()),
    ok.

%% @doc follow standard gen_server pattern of declaring a local start_link implementation
%%      which itself calls gen_server:start_link/3 or gen_server:start_link/4.
start_link() -> 
    %gen_server:start_link(?MODULE, [], [{debug, [log, {log_to_file, "kvs.log"}]}]).
    gen_server:start_link(?MODULE, [], []).

%% @doc called during code changes, which this implementation ignores.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @doc handles system messages, including timeouts.
handle_info(timeout, State) ->
    Writes2 = lists:filter(fun filter_writes/1, State#kvs_store.pending_writes),
    Reads2 = lists:filter(fun filter_reads/1, State#kvs_store.pending_reads),
    {noreply, State#kvs_store{pending_writes=Writes2, pending_reads=Reads2}, ?KVS_POLL_PENDING};
handle_info(_Info, State) ->
    {noreply, State, ?KVS_POLL_PENDING}.

%%
%% All server->server communcation occurs via asynchronous calls
%%

%% @doc handle asynchronous communication.
handle_cast({retrieve, Sender, Client, Key}, State=#kvs_store{data=Data}) ->
    gen_server:cast(Sender, {retrieved, self(), Client, Key, proplists:get_value(Key, Data)}),
    {noreply, State, ?KVS_POLL_PENDING};

handle_cast({retrieved, _Sender, Client, Key, Value}, Store=#kvs_store{pending_reads=Reads}) ->
    case proplists:get_value({Client, Key}, Reads) of
	undefined ->
	    {noreply, Store, ?KVS_POLL_PENDING};
	{0, Values, _Timestamp} ->
	    Freq = lists:foldr(fun(X, Acc) ->
				       case proplists:get_value(X, Acc) of
					   undefined -> [{X, 1} | Acc];
					   N -> [{X, N+1} | proplists:delete(X, Acc)]
				       end
			       end, [], Values),
	    [{Popular, _} | _ ] = lists:reverse(lists:keysort(2, Freq)),
	    gen_server:reply(Client, {ok, Popular}),
	    {noreply, (Store#kvs_store{pending_reads=proplists:delete({Key, Value}, Reads)}), ?KVS_POLL_PENDING};
	{Count, Values, Timestamp} ->
	    {noreply, Store#kvs_store{
		    pending_reads=[{{Client, Key}, {Count-1, [Value | Values], Timestamp}} |
				   proplists:delete({Client, Key}, Reads)]},
	    ?KVS_POLL_PENDING}
    end;

handle_cast({update, Sender, Client, Key, Value}, Store=#kvs_store{data=Data}) ->
    % sent to all nodes by first receiving node
    gen_server:cast(Sender, {updated, self(), Client, Key, Value}),
    {noreply, Store#kvs_store{data=[{Key, Value} | proplists:delete(Key, Data)]}, ?KVS_POLL_PENDING};


handle_cast({updated, _Sender, Client, Key, Value}, Store=#kvs_store{pending_writes=Writes}) ->
    {Count, Timestamp} = proplists:get_value({Client, Key}, Writes),
    case Count of
	undefined ->
	    {noreply, Store, ?KVS_POLL_PENDING};
	0 ->
	    gen_server:reply(Client, {ok, updated}),
	    {noreply, Store#kvs_store{pending_writes=proplists:delete({Key, Value}, Writes)}, ?KVS_POLL_PENDING};
	_ ->
	    {noreply, Store#kvs_store{
		    pending_writes=[{{Client, Key}, {Count-1, Timestamp}} |
				    proplists:delete({Client, Key}, Writes)]},
	    ?KVS_POLL_PENDING}
    end.

%%
%% All client->server communication occurs via synchronous calls.
%% 

%% @doc handle explicit stop requests
handle_call(stop, _Sender, State) ->
    {stop, stopped, stopped, State};

handle_call({get, Key}, Sender, Store=#kvs_store{pending_reads=Reads}) ->
    lists:foreach(fun(Pid) ->
			  gen_server:cast(Pid, {retrieve, self(), Sender, Key})
		  end, pg2:get_members(kvs)),
    % ?KVS_READS is required # of nodes to read from
    % [] is used to collect read values
    Reads2 = [{{Sender, Key}, {?KVS_READS, [], ts()}} | Reads],
    {noreply, Store#kvs_store{pending_reads=Reads2}, ?KVS_POLL_PENDING};

handle_call({set, Key, Value}, Sender, Store=#kvs_store{pending_writes=Writes}) ->
    % client interface for updating values
    lists:foreach(fun(Pid) ->
			  gen_server:cast(Pid, {update, self(), Sender, Key, Value})
		  end, pg2:get_members(kvs)),
    Writes2 = [{{Sender, Key}, {?KVS_WRITES, ts()}} | Writes],
    {noreply, Store#kvs_store{pending_writes=Writes2}, ?KVS_POLL_PENDING}.

%%
%% Utility functions
%%

ts() ->
    {Mega, Sec, _} = erlang:now(),
    (Mega * 1000000) + Sec.

%% @doc filter expired writes from pending_writes.
filter_writes({{Client, _Key}, {_Count, Ts}}) ->
    Now = ts(),
    if Now > Ts + ?KVS_WRITE_TIMEOUT ->
	    gen_server:reply(Client, {error, write_failed}),
	    false;
       true  ->
	    true
    end.

%% @doc filter expired reads from pending_reads.
filter_reads({{Client, _Key}, {_Count, _Values, Ts}}) ->
    Now = ts(),
    if Now > Ts + ?KVS_READ_TIMEOUT ->
	    gen_server:reply(Client, {error, read_failed}),
	    false;
       true  ->
	    true
    end.
