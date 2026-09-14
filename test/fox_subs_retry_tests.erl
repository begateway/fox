-module(fox_subs_retry_tests).

-include_lib("eunit/include/eunit.hrl").
-include("fox.hrl").

subscription_retry_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun(Context) -> ?_test(consume_recovers(Context, fun denied/1)) end,
        fun(Context) -> ?_test(channel_down_recovers(Context, normal)) end,
        fun(Context) -> ?_test(channel_down_recovers(Context, killed)) end,
        fun(Context) -> ?_test(stale_retry_ignored(Context)) end,
        fun(Context) -> ?_test(open_channel_recovers(Context)) end,
        fun(Context) -> ?_test(callback_amqp_failure_stops(Context)) end,
        fun(Context) -> ?_test(callback_bug_stops(Context)) end,
        fun(Context) -> ?_test(cleanup_exception_is_safe(Context)) end,
        fun(Context) -> ?_test(callback_badmatch_amqp_stops(Context)) end,
        fun(Context) -> ?_test(callback_invalid_result_stops(Context)) end,
        fun(Context) -> ?_test(consume_recovers(Context, fun(_) -> error(consume_bug) end)) end,
        fun(Context) -> ?_test(consume_recovers(Context, fun(_) -> {error, invalid_request} end)) end
    ]}.

setup() ->
    %% Existing integration-style EUnit tests leave pools running. Stop them
    %% before replacing VM-global AMQP modules. Each test starts its own worker.
    application:stop(fox),
    meck:new([amqp_connection, amqp_channel, fox_conn_worker,
        fox_conn_pool, fox_priv_utils], [passthrough]),
    meck:new(retry_callback, [non_strict]),
    T = ets:new(retry_test, [public]),
    Conn = spawn(fun idle/0),
    meck:expect(fox_conn_worker, register_subscriber, fun(_, _) -> ok end),
    meck:expect(fox_conn_pool, save_subs_meta, fun(_, _) -> ok end),
    meck:expect(fox_priv_utils, reconnect, fun(_, Message) ->
        erlang:send_after(100, self(), Message),
        ok
    end),
    meck:expect(amqp_connection, open_channel, fun(_) ->
        Channel = spawn(fun idle/0),
        ets:insert(T, {Channel}),
        {ok, Channel}
    end),
    expect_consume(),
    meck:expect(amqp_channel, call, fun(_, #'basic.cancel'{}, none) ->
        #'basic.cancel_ok'{}
    end),
    meck:expect(amqp_channel, close, fun(Ch) -> Ch ! stop, ok end),
    meck:expect(retry_callback, init, fun(_, _) -> {ok, undefined} end),
    meck:expect(retry_callback, terminate, fun(_, undefined) -> ok end),
    S = #subscription{pool_name = retry_test, conn_worker = self(),
        basic_consume = #'basic.consume'{queue = <<"retry">>},
        subs_module = retry_callback, subs_args = []},
    {ok, Pid} = fox_subs_worker:start_link(S, []),
    unlink(Pid),
    {Pid, Conn, T}.

cleanup({Pid, Conn, T}) ->
    fox_subs_worker:stop(Pid),
    Conn ! stop,
    [Ch ! stop || {Ch} <- ets:tab2list(T)],
    ets:delete(T),
    meck:unload(),
    ok.

consume_recovers({Pid, Conn, _}, Failure) ->
    expect_consume_failure(Failure),
    fox_subs_worker:connection_established(Pid, Conn),
    Failed = sys:get_state(Pid),
    assert_clean(Failed),
    ?assertEqual(1, Failed#subscription.retry_attempt),
    ?assertEqual(1, meck:num_calls(amqp_channel, close, '_')),
    ?assertEqual(1, meck:num_calls(retry_callback, terminate, '_')),
    expect_consume(),
    ?assertEqual(0, (await_subscription(Pid))#subscription.retry_attempt).

channel_down_recovers({Pid, Conn, _}, Reason) ->
    fox_subs_worker:connection_established(Pid, Conn),
    #subscription{channel = OldCh, channel_ref = Ref} = sys:get_state(Pid),
    Pid ! {'DOWN', Ref, process, OldCh, Reason},
    await(fun() -> (sys:get_state(Pid))#subscription.channel =/= OldCh end),
    #subscription{channel = NewCh} = await_subscription(Pid),
    ?assertNotEqual(OldCh, NewCh),
    ?assertEqual(1, meck:num_calls(retry_callback, terminate, '_')).

stale_retry_ignored({Pid, Conn, _}) ->
    expect_consume_failure(fun denied/1),
    fox_subs_worker:connection_established(Pid, Conn),
    Conn2 = spawn(fun idle/0),
    try
        fox_subs_worker:connection_established(Pid, Conn2),
        Current = sys:get_state(Pid),
        Count = meck:num_calls(amqp_connection, open_channel, '_'),
        Pid ! {subscribe_retry, Conn},
        Pid ! {'DOWN', make_ref(), process, self(), normal},
        ?assertEqual(Current, sys:get_state(Pid)),
        ?assertEqual(Count, meck:num_calls(amqp_connection, open_channel, '_')),
        expect_consume(),
        Success = await_subscription(Pid),
        Pid ! {subscribe_retry, Conn2},
        ?assertEqual(Success, sys:get_state(Pid))
    after Conn2 ! stop end.

open_channel_recovers({Pid, Conn, _}) ->
    meck:expect(amqp_connection, open_channel, 1, meck:seq([
        {error, closing}, {ok, Conn}
    ])),
    fox_subs_worker:connection_established(Pid, Conn),
    assert_clean(sys:get_state(Pid)),
    ?assertEqual(0, meck:num_calls(retry_callback, init, '_')),
    ?assertEqual(0, (await_subscription(Pid))#subscription.retry_attempt).

callback_amqp_failure_stops(Context) ->
    callback_init_failure_stops(Context, fun denied/1).

callback_bug_stops(Context) ->
    callback_init_failure_stops(Context, fun(_Channel) -> error(callback_bug) end).

cleanup_exception_is_safe(Context) ->
    meck:expect(retry_callback, terminate, fun(_, _) -> error(cleanup_bug) end),
    consume_recovers(Context, fun denied/1).

callback_badmatch_amqp_stops(Context) ->
    callback_init_failure_stops(Context, fun(_Channel) ->
        error({badmatch, {error, {channel_closed,
            {server_initiated_close, 403, <<"ACCESS_REFUSED">>}}}})
    end).

callback_invalid_result_stops(Context) ->
    callback_init_failure_stops(Context, fun(_Channel) -> invalid_state end).

callback_init_failure_stops({Pid, Conn, _}, Fail) ->
    meck:expect(retry_callback, init, fun(Channel, _) -> Fail(Channel) end),
    Ref = monitor(process, Pid),
    fox_subs_worker:connection_established(Pid, Conn),
    receive
        {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 1000 -> error(worker_did_not_stop)
    end,
    ?assertEqual(0, meck:num_calls(amqp_channel, close, '_')),
    ?assertEqual(0, meck:num_calls(retry_callback, terminate, '_')).

expect_consume() ->
    meck:expect(amqp_channel, subscribe, fun(_, _, _) ->
        #'basic.consume_ok'{consumer_tag = <<"retry-test">>}
    end).

expect_consume_failure(Failure) ->
    meck:expect(amqp_channel, subscribe,
        fun(Channel, _, _) -> Failure(Channel) end).

denied(Channel) ->
    exit({{shutdown, {server_initiated_close, 403, <<"ACCESS_REFUSED">>}},
          {gen_server, call, [Channel, subscribe, infinity]}}).

assert_clean(State) ->
    ?assertMatch(#subscription{channel = undefined, channel_ref = undefined,
        subs_state = undefined}, State).

await_subscription(Pid) ->
    await(fun() -> is_pid((sys:get_state(Pid))#subscription.channel) end),
    sys:get_state(Pid).

await(Predicate) -> await(Predicate, 200).
await(_, 0) -> error(await_timeout);
await(Predicate, Remaining) ->
    case Predicate() of
        true -> ok;
        false -> timer:sleep(10), await(Predicate, Remaining - 1)
    end.

idle() -> receive stop -> ok end.
