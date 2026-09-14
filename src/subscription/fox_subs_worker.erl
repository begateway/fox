-module(fox_subs_worker).
-behavior(gen_server).

-export([start_link/2, connection_established/2, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("otp_types.hrl").
-include("fox.hrl").

-callback init(Channel :: pid(), Args :: list()) -> {ok, State :: term()}.
-callback handle(Msg :: term(), Channel :: pid(), State :: term()) -> {ok, State :: term()}.
-callback terminate(Channel :: pid(), State :: term()) -> ok.


%%% module API

-spec start_link(#subscription{}, [gen_server:start_opt()]) -> gs_start_link_reply().
start_link(State, StartOptions) ->
    gen_server:start_link(?MODULE, State, StartOptions).


-spec connection_established(pid(), pid() | undefined) -> ok.
connection_established(Pid, Conn) ->
    gen_server:cast(Pid, {connection_established, Conn}).


-spec stop(pid()) -> ok.
stop(Pid) ->
    try
        gen_server:call(Pid, stop)
    catch
        exit:{noproc, _} -> ok
    end.


%%% gen_server API

-spec init(gs_args()) -> gs_init_reply().
init(#subscription{ref = SubsRef, pool_name = PoolName, conn_worker = CPid} = State) ->
    logger:info("~s init", [worker_name(State)]),
    put('$module', ?MODULE),
    fox_conn_worker:register_subscriber(CPid, self()),

    SubsMeta = #subs_meta{
        ref = SubsRef,
        conn_worker = CPid,
        subs_worker = self()
    },
    fox_conn_pool:save_subs_meta(PoolName, SubsMeta),
    {ok, State}.


-spec handle_call(gs_request(), gs_from(), gs_reply()) -> gs_call_reply().
handle_call(stop, _From, State) ->
    State2 = unsubscribe(State),
    logger:info("~s stop", [worker_name(State)]),
    {stop, normal, ok, State2};

handle_call(Any, _From, State) ->
    logger:error("unknown call ~w in ~p", [Any, ?MODULE]),
    {noreply, State}.


-spec handle_cast(gs_request(), gs_state()) -> gs_cast_reply().
handle_cast({connection_established, undefined}, State) ->
    {noreply, State};

handle_cast({connection_established, Conn},
    #subscription{connection = Conn} = State) ->
    %% Must not disturb an active subscription or retry
    {noreply, State};

handle_cast({connection_established, Conn}, State) ->
    logger:info("~s connection_established Conn:~p", [worker_name(State), Conn]),
    State2 = unsubscribe(State),
    {noreply, subscribe(State2#subscription{connection = Conn, retry_attempt = 0})};


handle_cast(Any, State) ->
    logger:error("~s unknown cast ~w", [worker_name(State), Any]),
    {noreply, State}.


-spec handle_info(gs_request(), gs_state()) -> gs_info_reply().
handle_info(#'basic.consume_ok'{} = Msg, State) ->
    {noreply, handle(Msg, State)};

handle_info({#'basic.deliver'{}, #amqp_msg{}} = Msg, State) ->
    {noreply, handle(Msg, State)};

handle_info(#'basic.cancel'{} = Msg, State) ->
    {noreply, handle(Msg, State)};

handle_info({subscribe_retry, Conn},
            #subscription{connection = Conn, channel = undefined} = State) ->
    {noreply, subscribe(State)};

handle_info({subscribe_retry, _Conn}, State) ->
    {noreply, State};

handle_info({'DOWN', Ref, process, Channel, Reason},
            #subscription{
                channel = Channel,
                channel_ref = Ref
            } = State) when is_reference(Ref) ->
    {noreply, retry({channel_down, Reason}, State)};

handle_info({'DOWN', _Ref, process, _Channel, _Reason}, State) ->
    {noreply, State};

handle_info(Request, State) ->
    logger:error("~s unknown info ~w", [worker_name(State), Request]),
    {noreply, State}.


-spec terminate(terminate_reason(), gs_state()) -> ok.
terminate(Reason, State) ->
    unsubscribe(State),
    fox_priv_utils:error_or_info(Reason, "~s terminated with reason ~w", [worker_name(State), Reason]),
    ok.


-spec code_change(term(), term(), term()) -> gs_code_change_reply().
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.



handle(Msg,
    #subscription{
        channel = Channel,
        subs_module = Module,
        subs_state = SubsState}
        = State) ->
    logger:info("~s handle event ~p", [worker_name(State), element(1, Msg)]),
    {ok, SubsState2} = Module:handle(Msg, Channel, SubsState),
    State#subscription{subs_state = SubsState2}.


%%% inner functions

worker_name(
  #subscription{
     pool_name = PoolName,
     connection = Conn,
     channel = Channel,
     basic_consume = BasicConsume
}) ->
    #'basic.consume'{queue = QueueName} = BasicConsume,
    FullName = io_lib:format(
                 "fox_subs_worker/~s/~s/Conn:~p/Channel:~p",
                 [PoolName, QueueName, Conn, Channel]
                ),
    unicode:characters_to_binary(FullName).

subscribe(#subscription{connection = Conn} = State) ->
    case is_process_alive(Conn) of
        false -> State;
        true -> open_channel(State)
    end.

open_channel(#subscription{
    connection = Conn,
    subs_module = Module,
    subs_args = Args}
    = State) ->
    case catch amqp_connection:open_channel(Conn) of
        {ok, Channel} when is_pid(Channel) ->
            Ref = erlang:monitor(process, Channel),
            {ok, SubsState} = Module:init(Channel, Args),
            consume(State#subscription{
                        channel = Channel,
                        channel_ref = Ref,
                        subs_state = SubsState});
        Failure ->
            retry({open_channel, Failure}, State)
    end.

consume(#subscription{
    channel = Channel,
    basic_consume = BasicConsume}
    = State) ->
    case catch amqp_channel:subscribe(Channel, BasicConsume, self()) of
        #'basic.consume_ok'{consumer_tag = Tag} ->
            logger:info("~s has subscribed to queue", [worker_name(State)]),
            State#subscription{subs_tag = Tag, retry_attempt = 0};
        Failure ->
            retry({basic_consume, Failure}, State)
    end.

retry(Reason,
    #subscription{
        connection = Conn,
        retry_attempt = Attempt}
        = State) ->
    State2 = unsubscribe(State),
    case is_process_alive(Conn) of
        false -> State2;
        true ->
            logger:warning("~s subscription retry attempt ~p: ~w",
                           [worker_name(State2), Attempt + 1, Reason]),
            fox_priv_utils:reconnect(Attempt, {subscribe_retry, Conn}),
            State2#subscription{retry_attempt = Attempt + 1}
    end.

unsubscribe(#subscription{channel = undefined} = State) ->
    State;
unsubscribe(#subscription{
    channel = Channel,
    channel_ref = Ref,
    subs_module = Module,
    subs_state = SubsState,
    subs_tag = Tag}
    = State) ->
    logger:info("~s unsubscribe from queue", [worker_name(State)]),
    erlang:demonitor(Ref, [flush]),
    case Tag of
        undefined -> ok;
        _ -> fox_utils:channel_call(Channel, #'basic.cancel'{consumer_tag = Tag})
    end,
    terminate_callback(Module, Channel, SubsState, State),
    fox_priv_utils:close_channel(Channel),
    State#subscription{
        channel = undefined,
        channel_ref = undefined,
        subs_state = undefined,
        subs_tag = undefined
    }.

terminate_callback(Module, Channel, SubsState, State) ->
    case catch Module:terminate(Channel, SubsState) of
        {'EXIT', Reason} ->
            logger:error("~s callback terminate failed: ~p", [worker_name(State), Reason]);
        _ -> ok
    end.
