%%% @doc Cowboy WebSocket handler for the HyperBEAM WebSocket device.
%%% Manages WebSocket connections, topic subscriptions, and message broadcasting.
%%%
%%% This module handles:
%%% - WebSocket connection lifecycle (init, websocket_init, websocket_handle, websocket_info)
%%% - Topic subscription parsing from URL query parameters
%%% - Message broadcasting to subscribed clients
%%% - Connection cleanup on disconnection
-module(hb_websocket_handler).
-behaviour(cowboy_websocket).

%% Cowboy WebSocket behavior callbacks
-export([init/2, websocket_init/1, websocket_handle/2, websocket_info/2, terminate/3]).

-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

-record(state, {
    topics = [],        % List of topics this connection subscribes to
    connection_pid      % PID of this connection process
}).

%% @doc Initialize the WebSocket upgrade
init(Req, _State) ->
    ?event(websocket, {connection_init, {method, cowboy_req:method(Req)}, {path, cowboy_req:path(Req)}}),
    
    % Parse topics from query parameters
    QsVals = cowboy_req:parse_qs(Req),
    Topics = parse_topics(QsVals),
    
    ?event(websocket, {connection_topics, {topics, Topics}}),
    
    % Upgrade to WebSocket
    {cowboy_websocket, Req, #state{topics = Topics, connection_pid = self()}}.

%% @doc Initialize WebSocket connection after upgrade
websocket_init(State) ->
    ?event(websocket, {websocket_init, {pid, self()}, {topics, State#state.topics}}),
    
    % Ensure the WebSocket device server is started
    case dev_websocket:start() of
        {ok, _} -> ok;
        {error, Reason} -> 
            ?event(websocket, {failed_to_start_device, {reason, Reason}})
    end,
    
    % Register this connection with the WebSocket device
    case State#state.topics of
        [] -> 
            ?event(websocket, {no_topics_to_subscribe, {connection_pid, self()}});
        Topics ->
            ?event(websocket, {registering_connection, {connection_pid, self()}, {topics, Topics}}),
            dev_websocket:add_subscription(self(), Topics),
            ?event(websocket, {connection_registered_successfully, {connection_pid, self()}, {topics, Topics}})
    end,
    
    % Send initial connection confirmation
    WelcomeMsg = #{
        <<"type">> => <<"connection_established">>,
        <<"subscribed_topics">> => State#state.topics,
        <<"timestamp">> => erlang:system_time(millisecond)
    },
    
    {reply, {text, encode_json(WelcomeMsg)}, State}.

%% @doc Handle incoming WebSocket messages from client
websocket_handle({text, Data}, State) ->
    ?event(websocket, {received_message, {data, Data}}),
    
    try
        Msg = decode_json(Data),
        handle_client_message(Msg, State)
    catch
        _:Error ->
            ?event(websocket, {invalid_json, {error, Error}, {data, Data}}),
            ErrorResponse = #{
                <<"type">> => <<"error">>,
                <<"message">> => <<"Invalid JSON">>,
                <<"timestamp">> => erlang:system_time(millisecond)
            },
            {reply, {text, encode_json(ErrorResponse)}, State}
    end;

websocket_handle({binary, Data}, State) ->
    ?event(websocket, {received_binary_data, {size, byte_size(Data)}}),
    % For now, we don't handle binary data - convert to text and process
    websocket_handle({text, Data}, State);

websocket_handle(_Frame, State) ->
    ?event(websocket, {unhandled_frame}),
    {ok, State}.

%% @doc Handle messages sent to this WebSocket process
websocket_info({websocket_message, Topic, EncodedMsg}, State) ->
    ?event(websocket, {received_broadcast_message, {connection_pid, self()}, {topic, Topic}, {subscribed_topics, State#state.topics}}),
    % Check if this connection is subscribed to the topic
    case lists:member(Topic, State#state.topics) of
        true ->
            ?event(websocket, {sending_to_client, {connection_pid, self()}, {topic, Topic}}),
            BroadcastMsg = #{
                <<"type">> => <<"message">>,
                <<"topic">> => Topic,
                <<"data">> => decode_json_safe(EncodedMsg),
                <<"timestamp">> => erlang:system_time(millisecond)
            },
            ?event(websocket, {message_sent_to_client, {connection_pid, self()}, {topic, Topic}}),
            {reply, {text, encode_json(BroadcastMsg)}, State};
        false ->
            ?event(websocket, {ignoring_message_not_subscribed, {connection_pid, self()}, {topic, Topic}, {subscribed_topics, State#state.topics}}),
            {ok, State}
    end;

websocket_info(Info, State) ->
    ?event(websocket, {unhandled_info, {info, Info}}),
    {ok, State}.

%% @doc Handle WebSocket connection termination
terminate(Reason, _PartialReq, State) ->
    ?event(websocket, {connection_terminated, {reason, Reason}, {topics, State#state.topics}}),
    
    % Unsubscribe from all topics
    case State#state.topics of
        [] -> ok;
        Topics ->
            dev_websocket:remove_subscription(self(), Topics)
    end,
    
    ok.

%% Internal functions

%% @doc Parse topics from query string parameters
parse_topics(QsVals) ->
    case lists:keyfind(<<"topic">>, 1, QsVals) of
        {<<"topic">>, TopicString} ->
            % Split comma-separated topics
            TopicList = binary:split(TopicString, <<",">>, [global]),
            % Trim whitespace and filter empty strings
            [string:trim(Topic) || Topic <- TopicList, byte_size(string:trim(Topic)) > 0];
        false ->
            []
    end.

%% @doc Handle client messages (subscription changes, etc.)
handle_client_message(Msg, State) when is_map(Msg) ->
    case maps:get(<<"type">>, Msg, undefined) of
        <<"subscribe">> ->
            handle_subscribe(Msg, State);
        <<"unsubscribe">> ->
            handle_unsubscribe(Msg, State);
        <<"ping">> ->
            handle_ping(Msg, State);
        _ ->
            ErrorResponse = #{
                <<"type">> => <<"error">>,
                <<"message">> => <<"Unknown message type">>,
                <<"timestamp">> => erlang:system_time(millisecond)
            },
            {reply, {text, encode_json(ErrorResponse)}, State}
    end;

handle_client_message(_, State) ->
    ErrorResponse = #{
        <<"type">> => <<"error">>,
        <<"message">> => <<"Message must be a JSON object">>,
        <<"timestamp">> => erlang:system_time(millisecond)
    },
    {reply, {text, encode_json(ErrorResponse)}, State}.

%% @doc Handle subscription requests
handle_subscribe(Msg, State) ->
    case maps:get(<<"topics">>, Msg, []) of
        NewTopics when is_list(NewTopics) ->
            % Filter out topics already subscribed to
            TopicsToAdd = [T || T <- NewTopics, not lists:member(T, State#state.topics)],
            case TopicsToAdd of
                [] ->
                    Response = #{
                        <<"type">> => <<"subscription_response">>,
                        <<"message">> => <<"Already subscribed to all requested topics">>,
                        <<"subscribed_topics">> => State#state.topics,
                        <<"timestamp">> => erlang:system_time(millisecond)
                    },
                    {reply, {text, encode_json(Response)}, State};
                _ ->
                    dev_websocket:add_subscription(self(), TopicsToAdd),
                    NewState = State#state{topics = lists:usort(State#state.topics ++ TopicsToAdd)},
                    Response = #{
                        <<"type">> => <<"subscription_response">>,
                        <<"message">> => <<"Successfully subscribed">>,
                        <<"new_topics">> => TopicsToAdd,
                        <<"subscribed_topics">> => NewState#state.topics,
                        <<"timestamp">> => erlang:system_time(millisecond)
                    },
                    {reply, {text, encode_json(Response)}, NewState}
            end;
        _ ->
            ErrorResponse = #{
                <<"type">> => <<"error">>,
                <<"message">> => <<"Topics must be a list">>,
                <<"timestamp">> => erlang:system_time(millisecond)
            },
            {reply, {text, encode_json(ErrorResponse)}, State}
    end.

%% @doc Handle unsubscription requests
handle_unsubscribe(Msg, State) ->
    case maps:get(<<"topics">>, Msg, []) of
        TopicsToRemove when is_list(TopicsToRemove) ->
            % Filter to only topics we're actually subscribed to
            ActualTopicsToRemove = [T || T <- TopicsToRemove, lists:member(T, State#state.topics)],
            case ActualTopicsToRemove of
                [] ->
                    Response = #{
                        <<"type">> => <<"unsubscription_response">>,
                        <<"message">> => <<"Not subscribed to any of the requested topics">>,
                        <<"subscribed_topics">> => State#state.topics,
                        <<"timestamp">> => erlang:system_time(millisecond)
                    },
                    {reply, {text, encode_json(Response)}, State};
                _ ->
                    dev_websocket:remove_subscription(self(), ActualTopicsToRemove),
                    NewTopics = lists:subtract(State#state.topics, ActualTopicsToRemove),
                    NewState = State#state{topics = NewTopics},
                    Response = #{
                        <<"type">> => <<"unsubscription_response">>,
                        <<"message">> => <<"Successfully unsubscribed">>,
                        <<"removed_topics">> => ActualTopicsToRemove,
                        <<"subscribed_topics">> => NewState#state.topics,
                        <<"timestamp">> => erlang:system_time(millisecond)
                    },
                    {reply, {text, encode_json(Response)}, NewState}
            end;
        _ ->
            ErrorResponse = #{
                <<"type">> => <<"error">>,
                <<"message">> => <<"Topics must be a list">>,
                <<"timestamp">> => erlang:system_time(millisecond)
            },
            {reply, {text, encode_json(ErrorResponse)}, State}
    end.

%% @doc Handle ping requests
handle_ping(_Msg, State) ->
    PongResponse = #{
        <<"type">> => <<"pong">>,
        <<"timestamp">> => erlang:system_time(millisecond)
    },
    {reply, {text, encode_json(PongResponse)}, State}.

%% @doc Encode JSON safely
encode_json(Data) ->
    try
        hb_json:encode(Data)
    catch
        _:_ ->
            % Fallback encoding
            iolist_to_binary(io_lib:format("~p", [Data]))
    end.

%% @doc Decode JSON safely
decode_json(Data) ->
    hb_json:decode(Data).

%% @doc Decode JSON safely without throwing errors
decode_json_safe(Data) ->
    try
        hb_json:decode(Data)
    catch
        _:_ -> Data
    end.

%% EUnit Tests

-ifdef(TEST).

parse_topics_test() ->
    % Test single topic
    ?assertEqual([<<"topic1">>], parse_topics([{<<"topic">>, <<"topic1">>}])),
    
    % Test multiple topics
    ?assertEqual([<<"topic1">>, <<"topic2">>], parse_topics([{<<"topic">>, <<"topic1,topic2">>}])),
    
    % Test topics with spaces
    ?assertEqual([<<"topic1">>, <<"topic2">>], parse_topics([{<<"topic">>, <<" topic1 , topic2 ">>}])),
    
    % Test no topics
    ?assertEqual([], parse_topics([])),
    
    % Test empty topic string
    ?assertEqual([], parse_topics([{<<"topic">>, <<"">>}])).

encode_decode_json_test() ->
    TestData = #{<<"test">> => <<"value">>, <<"number">> => 42},
    Encoded = encode_json(TestData),
    ?assert(is_binary(Encoded)),
    Decoded = decode_json(Encoded),
    ?assertEqual(TestData, Decoded).

-endif.