-module(kafkamocker_demo).
-export([start/0,
         metadata1/0, metadata2/0,
         change_kafka_config/0, toggle_kafka_config/1,
         ask_broker/2]).

-include("kafkamocker.hrl").

-define(TEST_TOPIC, <<"ekaf">>).

metadata1()->
    Topics = [?TEST_TOPIC],
    #kafkamocker_metadata{ brokers =
                           [ #kafkamocker_broker{ id = 1, host = "localhost", port = 9907 }],
                           topics =
                           [#kafkamocker_topic { name = Topic,
                                                 partitions =
                                                 [ #kafkamocker_partition { id = 0, leader = 1,
                                                                            replicas = [#kafkamocker_replica{ id = 1 }],
                                                                            isrs = [#kafkamocker_isr{ id = 1 }]
                                                                           }
                                                  ]
                                                }
                            || Topic <- Topics]
                          }.

metadata2()->
    Topics = [?TEST_TOPIC],
    #kafkamocker_metadata{ brokers = [ #kafkamocker_broker{ id = 1, host = "localhost", port = 9907 },
                           #kafkamocker_broker{ id = 2, host = "localhost", port = 9908 },
                           #kafkamocker_broker{ id = 3, host = "localhost", port = 9909 }],
               topics =  [ #kafkamocker_topic { name = Topic,
                                    partitions = [ #kafkamocker_partition { id = 0, leader = 1,
                                                                replicas = [#kafkamocker_replica{ id = 1 }],
                                                                isrs = [#kafkamocker_isr{ id = 1 }]
                                                               },
                                                   #kafkamocker_partition { id = 1, leader = 2,
                                                                replicas = [#kafkamocker_replica{ id = 2 }],
                                                                isrs = [#kafkamocker_isr{ id = 2 }]
                                                               },
                                                   #kafkamocker_partition { id = 3, leader = 3,
                                                                replicas = [#kafkamocker_replica{ id = 3 }],
                                                                isrs = [#kafkamocker_isr{ id = 3 }]
                                                               }
                                                  ]
                                   }
                            || Topic <- Topics]
              }.


start()->
    application:set_env(kafkamocker, kafkamocker_bootstrap_topics, [<<"ekaf">>]),
    application:set_env(kafkamocker, kafkamocker_bootstrap_broker, {"localhost",9907}),
    Pid = spawn( fun() -> kafka_consumer_loop([],{self(),0}) end),
    erlang:register(kafka_consumer, Pid),
    application:set_env(kafkamocker, kafkamocker_callback, kafka_consumer),
    application:ensure_started(kafkamocker),
    application:start(ranch),
    application:start(kafkamocker),
    kafkamocker_fsm:start_link({metadata, ?MODULE:metadata1()}),
    ok.

change_kafka_config()->
    gen_fsm:sync_send_event(kafkamocker_fsm, {metadata, ?MODULE:metadata2()}).

toggle_kafka_config(Every)->
    spawn(fun()-> loop_start_stop(started, Every) end).


kafka_consumer_loop(Acc,{From,Stop}=Acc2)->
    receive
        {stop, N}->
            kafka_consumer_loop(Acc, {From,N});
        stop ->
            io:format("kafka_consumer_loop stopping",[]),
            ok;
        {flush, NewFrom} ->
            %io:format("asked to flush when consumer got ~w items",[length(Acc)]),
            % kafka_consumer_loop should flush
            NewFrom ! {flush, Acc },
            kafka_consumer_loop([], {NewFrom,0});
        {flush, NewStop, NewFrom} ->
            % io:format("~p asked to flush when consumer got ~w items",[ekaf_utils:epoch(), length(Acc)]),
            % kafka_consumer_loop should flush
            NextAcc =
                case length(Acc) of
                    NewStop ->
                        NewFrom ! {flush, Acc },
                        [];
                    _ ->
                        Acc
            end,
            kafka_consumer_loop(NextAcc, {NewFrom,NewStop});
        {info, From} ->
            %kafka_consumer_loop should reply
            From ! {info, Acc },
            kafka_consumer_loop(Acc, Acc2);
        {produce,X} ->
            io:format("kafka_consumer_loop INCOMING ~p",[ X ]),
            Next = Acc++X,
            Next2 =
                case length(Next) of
                    Stop ->
                        From ! {flush, Next },
                        [];
                    _ ->
                        Next
                end,
            kafka_consumer_loop(Next2, Acc2);
        _E ->
            io:format("kafka_consumer_loop unexp: ~p",[_E]),
            kafka_consumer_loop(Acc, Acc2)
    end.

loop_start_stop(State, Timeout)->
    receive
    after Timeout ->
            case State of
                started ->
                    io:format("|",[]),
                    [ kafkamocker_complex:ask_broker(X, stop) || X <- [2,3] ],
                    loop_start_stop(stopped, Timeout);
                _ ->
                    io:format("_",[]),
                    [ kafkamocker_complex:ask_broker(X, start) || X <- [2,3] ],
                    loop_start_stop(started, Timeout)
            end
    end.


ask_broker(1, Cmd) ->
    gen_fsm:send_event(kafkamocker_fsm, {broker, Cmd, #kafkamocker_broker{ id = 1, host = "localhost", port = 9907 }});
ask_broker(2, Cmd) ->
    gen_fsm:send_event(kafkamocker_fsm, {broker, Cmd, #kafkamocker_broker{ id = 2, host = "localhost", port = 9908 }});
ask_broker(3, Cmd) ->
    gen_fsm:send_event(kafkamocker_fsm, {broker, Cmd, #kafkamocker_broker{ id = 3, host = "localhost", port = 9909 }}).
