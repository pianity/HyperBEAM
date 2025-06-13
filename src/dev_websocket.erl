%%% @doc WebSocket middleware device for broadcasting AO process messages in real-time.
%%% Provides a WebSocket endpoint that allows external subscribers to receive
%%% messages based on topic subscriptions.
%%%
%%% The device implements:
%%% - WebSocket server with topic-based subscription management
%%% - Asynchronous message broadcasting based on "Topic" tag
%%% - Automatic topic creation from incoming message tags
%%%
%%% WebSocket endpoint: ws://server:port/ws?topic=topic1,topic2
-module(dev_websocket).
-behaviour(gen_server).

%% Device API exports
-export([info/0, info/3, cast/2, cast/3]).

%% WebSocket server management
-export([start_link/0, start/0, stop/0]).
-export([broadcast_message/1, add_subscription/2, remove_subscription/2]).
-export([get_subscriptions/1, get_all_topics/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

-record(state, {
    connections = #{},  % #{ConnectionPid => [Topics]}
    topics = #{},       % #{Topic => [ConnectionPid]}
    server_info = #{}   % WebSocket server information
}).

%% @doc Device info function specifying exports and behavior
info() ->
    #{
        exports => [info, cast],
        excludes => []
    }.

%% @doc Handle info requests - provides WebSocket server URI and available topics
info(_Msg1, _Msg2, _Opts) ->
    {ok, #{
        uri => get_websocket_uri(),
        topics => get_all_topics(),
        device => <<"websocket@1.0">>,
        description => <<"WebSocket middleware device for real-time message broadcasting">>
    }}.

%% @doc Cast function that immediately acknowledges and broadcasts messages asynchronously (2-arg version)
cast(Msg, Opts) ->
    cast(Msg, #{}, Opts).

%% @doc Cast function that immediately acknowledges and broadcasts messages asynchronously (3-arg version)
cast(Msg, Req, Opts) ->
    % Extract the body from the request and parse it as JSON
    Body = hb_ao:get(<<"body">>, Req, <<>>, Opts),
    
    TargetMsg = case Body of
        <<>> -> Msg;  % No body, use original message
        _ ->
            try 
                ParsedBody = hb_json:decode(Body),
                ParsedBody
            catch
                _:Error ->
                    Msg  % Fallback to original message
            end
    end,
    
    % Ensure the WebSocket server is started
    ?event(websocket, {cast_called, {msg_keys, maps:keys(Msg)}, {target_keys, maps:keys(TargetMsg)}}),
    case start() of
        {ok, _} -> 
            ?event(websocket, {websocket_server_running});
        {error, Reason} -> 
            ?event(websocket, {websocket_server_start_failed, {reason, Reason}})
    end,
    % Immediately acknowledge the cast and broadcast the target message
    spawn(fun() -> 
        ?event(websocket, {spawning_broadcast_process}),
        broadcast_message(TargetMsg) 
    end),
    ?event(websocket, {cast_completed}),
    {ok, <<"OK">>}.

%% @doc Start the WebSocket server and manager
start() ->
    case whereis(?MODULE) of
        undefined ->
            ?event(websocket, {starting_websocket_server}),
            case start_link() of
                {ok, Pid} -> 
                    ?event(websocket, {websocket_server_started, {pid, Pid}}),
                    {ok, Pid};
                {error, {already_started, Pid}} -> 
                    ?event(websocket, {websocket_server_already_running, {pid, Pid}}),
                    {ok, Pid};
                Error -> 
                    ?event(websocket, {websocket_server_start_error, {error, Error}}),
                    Error
            end;
        Pid -> 
            ?event(websocket, {websocket_server_already_running, {pid, Pid}}),
            {ok, Pid}
    end.

%% @doc Start the WebSocket manager as a gen_server
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Stop the WebSocket server
stop() ->
    gen_server:call(?MODULE, stop).

%% @doc Broadcast a message to subscribed clients based on Topic tag
broadcast_message(Msg) ->
    case extract_topic(Msg) of
        {ok, Topic} ->
            ?event(websocket, {broadcasting_message, {topic, Topic}, {msg_id, maps:get(<<"id">>, Msg, undefined)}, {full_msg, Msg}}),
            gen_server:cast(?MODULE, {broadcast, Topic, Msg});
        {error, no_topic} ->
            ?event(websocket, {no_topic_found, {msg_keys, maps:keys(Msg)}, {msg, Msg}})
    end.

%% @doc Add a subscription for a connection to specific topics
add_subscription(ConnectionPid, Topics) when is_list(Topics) ->
    gen_server:call(?MODULE, {add_subscription, ConnectionPid, Topics}).

%% @doc Remove a subscription for a connection
remove_subscription(ConnectionPid, Topics) when is_list(Topics) ->
    gen_server:call(?MODULE, {remove_subscription, ConnectionPid, Topics}).

%% @doc Get subscriptions for a specific connection
get_subscriptions(ConnectionPid) ->
    gen_server:call(?MODULE, {get_subscriptions, ConnectionPid}).

%% @doc Get all available topics
get_all_topics() ->
    try
        gen_server:call(?MODULE, get_all_topics)
    catch
        exit:{noproc, _} -> []
    end.

%% @doc Get WebSocket URI based on current server configuration
get_websocket_uri() ->
    %% fix this port doesn't change based on the config
    Host = hb_opts:get(host, <<"localhost">>, #{}),
    Port = hb_opts:get(port, 8734, #{}),
    iolist_to_binary([<<"ws://">>, Host, <<":">>, integer_to_binary(Port), <<"/ws">>]).

%% @doc Extract topic from message tags
extract_topic(Msg) ->
    
    % First try tags
    case maps:get(<<"tags">>, Msg, undefined) of
        Tags when is_map(Tags) ->
            case maps:get(<<"Topic">>, Tags, undefined) of
                undefined -> 
                    case maps:get(<<"topic">>, Tags, undefined) of
                        undefined -> 
                            try_direct_topic(Msg);
                        Topic -> 
                            {ok, Topic}
                    end;
                Topic -> 
                    {ok, Topic}
            end;
        _ ->
            try_direct_topic(Msg)
    end.

%% @doc Try to find topic directly in message
try_direct_topic(Msg) ->
    case maps:get(<<"Topic">>, Msg, undefined) of
        undefined -> 
            case maps:get(<<"topic">>, Msg, undefined) of
                undefined -> 
                    {error, no_topic};
                Topic -> 
                    {ok, Topic}
            end;
        Topic -> 
            {ok, Topic}
    end.

%% gen_server callbacks

init([]) ->
    ?event(websocket, {starting_websocket_server}),
    State = #state{},
    {ok, State}.

handle_call({add_subscription, ConnectionPid, Topics}, _From, State) ->
    ?event(websocket, {client_subscribing, {pid, ConnectionPid}, {topics, Topics}}),
    NewState = add_connection_topics(ConnectionPid, Topics, State),
    monitor(process, ConnectionPid),
    ?event(websocket, {client_subscribed, {pid, ConnectionPid}, {topics, Topics}, {total_topics, maps:keys(NewState#state.topics)}, {total_connections, maps:size(NewState#state.connections)}}),
    {reply, ok, NewState};

handle_call({remove_subscription, ConnectionPid, Topics}, _From, State) ->
    ?event(websocket, {client_unsubscribing, {pid, ConnectionPid}, {topics, Topics}}),
    NewState = remove_connection_topics(ConnectionPid, Topics, State),
    ?event(websocket, {client_unsubscribed, {pid, ConnectionPid}, {topics, Topics}, {remaining_topics, maps:keys(NewState#state.topics)}}),
    {reply, ok, NewState};

handle_call({get_subscriptions, ConnectionPid}, _From, State) ->
    Subscriptions = maps:get(ConnectionPid, State#state.connections, []),
    {reply, Subscriptions, State};

handle_call(get_all_topics, _From, State) ->
    Topics = maps:keys(State#state.topics),
    {reply, Topics, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({broadcast, Topic, Msg}, State) ->
    case maps:get(Topic, State#state.topics, []) of
        [] ->
            ?event(websocket, {no_subscribers_for_topic, {topic, Topic}, {available_topics, maps:keys(State#state.topics)}});
        Subscribers ->
            ?event(websocket, {broadcasting_to_subscribers, {topic, Topic}, {subscriber_count, length(Subscribers)}, {subscriber_pids, Subscribers}}),
            EncodedMsg = encode_message(Msg),
            lists:foreach(fun(Pid) ->
                try
                    ?event(websocket, {sending_message_to_client, {pid, Pid}, {topic, Topic}}),
                    Pid ! {websocket_message, Topic, EncodedMsg},
                    ?event(websocket, {message_sent_successfully, {pid, Pid}, {topic, Topic}})
                catch
                    Error:Reason -> 
                        ?event(websocket, {failed_to_send_message, {pid, Pid}, {topic, Topic}, {error, Error}, {reason, Reason}})
                end
            end, Subscribers)
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    ?event(websocket, {connection_down, {pid, Pid}}),
    NewState = remove_connection(Pid, State),
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ?event(websocket, {websocket_server_stopping}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

%% @doc Add topics for a connection
add_connection_topics(ConnectionPid, Topics, State) ->
    % Add connection to topics mapping
    NewTopics = lists:foldl(fun(Topic, TopicsAcc) ->
        Subscribers = maps:get(Topic, TopicsAcc, []),
        case lists:member(ConnectionPid, Subscribers) of
            true -> TopicsAcc;
            false -> maps:put(Topic, [ConnectionPid | Subscribers], TopicsAcc)
        end
    end, State#state.topics, Topics),
    
    % Add topics to connection mapping
    ExistingTopics = maps:get(ConnectionPid, State#state.connections, []),
    NewConnectionTopics = lists:usort(Topics ++ ExistingTopics),
    NewConnections = maps:put(ConnectionPid, NewConnectionTopics, State#state.connections),
    
    State#state{
        connections = NewConnections,
        topics = NewTopics
    }.

%% @doc Remove topics for a connection
remove_connection_topics(ConnectionPid, Topics, State) ->
    % Remove connection from topics mapping
    NewTopics = lists:foldl(fun(Topic, TopicsAcc) ->
        case maps:get(Topic, TopicsAcc, []) of
            [] -> TopicsAcc;
            Subscribers ->
                NewSubscribers = lists:delete(ConnectionPid, Subscribers),
                case NewSubscribers of
                    [] -> maps:remove(Topic, TopicsAcc);
                    _ -> maps:put(Topic, NewSubscribers, TopicsAcc)
                end
        end
    end, State#state.topics, Topics),
    
    % Remove topics from connection mapping
    ExistingTopics = maps:get(ConnectionPid, State#state.connections, []),
    NewConnectionTopics = lists:subtract(ExistingTopics, Topics),
    NewConnections = case NewConnectionTopics of
        [] -> maps:remove(ConnectionPid, State#state.connections);
        _ -> maps:put(ConnectionPid, NewConnectionTopics, State#state.connections)
    end,
    
    State#state{
        connections = NewConnections,
        topics = NewTopics
    }.

%% @doc Remove all topics for a connection (when connection dies)
remove_connection(ConnectionPid, State) ->
    case maps:get(ConnectionPid, State#state.connections, []) of
        [] -> State;
        Topics -> remove_connection_topics(ConnectionPid, Topics, State)
    end.

%% @doc Encode message for WebSocket transmission
encode_message(Msg) ->
    try
        hb_json:encode(Msg)
    catch
        _:_ ->
            % Fallback to simple JSON encoding if hb_json fails
            iolist_to_binary(io_lib:format("~p", [Msg]))
    end.

%% EUnit Tests

-ifdef(TEST).

websocket_device_info_test() ->
    % Test the device info structure
    DeviceInfo = info(),
    ?assertMatch(#{exports := [info, cast], excludes := []}, DeviceInfo),
    
    % Test the actual info call
    {ok, Result} = info(#{}, #{}, #{}),
    ?assertMatch(#{uri := _, topics := _, device := <<"websocket@1.0">>}, Result).

websocket_cast_test() ->
    Msg = #{<<"test">> => <<"message">>},
    Result = cast(Msg, #{}),
    ?assertEqual({ok, <<"OK">>}, Result).

extract_topic_test() ->
    % Test with Topic tag
    Msg1 = #{<<"tags">> => #{<<"Topic">> => <<"test-topic">>}},
    ?assertEqual({ok, <<"test-topic">>}, extract_topic(Msg1)),
    
    % Test with lowercase topic tag
    Msg2 = #{<<"tags">> => #{<<"topic">> => <<"test-topic2">>}},
    ?assertEqual({ok, <<"test-topic2">>}, extract_topic(Msg2)),
    
    % Test with no topic
    Msg3 = #{<<"tags">> => #{<<"other">> => <<"value">>}},
    ?assertEqual({error, no_topic}, extract_topic(Msg3)),
    
    % Test with no tags
    Msg4 = #{<<"other">> => <<"value">>},
    ?assertEqual({error, no_topic}, extract_topic(Msg4)).

-endif.