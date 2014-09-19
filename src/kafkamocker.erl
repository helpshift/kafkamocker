-module(kafkamocker).

-compile([export_all]).
-include("kafkamocker.hrl").
-include_lib("eunit/include/eunit.hrl").

start()->
    application:start(ranch),
    application:start(kafkamocker),
    kafkamocker_fsm:start_link().

simple()->
    application:set_env(kafkamocker, kafkamocker_bootstrap_topics, [<<"ekaf">>]),
    application:set_env(kafkamocker, kafkamocker_bootstrap_broker, {"localhost",9907}),
    application:ensure_started(kafkamocker),
    start().

simulate_brokers_down()->
    send_event(simulate_brokers_down).

send_event(Event)->
    gen_fsm:send_event(kafkamocker_fsm, Event).

log(_A,_B)->
    error_logger:info_msg(_A,_B).

debug(_A,_B)->
    %format(A,B),
    ok.

format(A,B)->
    io:format(A,B).

metadata() ->
    {BrokerHost, BrokerPort} =
        case application:get_env(kafkamocker, kafkamocker_bootstrap_broker) of
            undefined -> {"localhost",9091};
            {ok, CustomBroker} -> CustomBroker
        end,
    Topics = case application:get_env(kafkamocker, kafkamocker_bootstrap_topics) of
                 undefined -> [<<"events">>];
                 {ok, CustomTopics} -> CustomTopics
             end,

    #kafkamocker_metadata{ brokers = [ #kafkamocker_broker{ id = 1, host = BrokerHost, port = BrokerPort }],
               topics =  [ #kafkamocker_topic { name = Topic,
                                    partitions = [ #kafkamocker_partition { id = 0, leader = 1,
                                                                replicas = [#kafkamocker_replica{ id = 1 }],
                                                                isrs = [#kafkamocker_isr{ id = 1 }]
                                                               }
                                                  ]
                                   }
                            || Topic <- Topics]
              }.

produce()->
    #kafkamocker_produce_request{
         cor_id = 1, timeout = 100,
         topics = [
                   #kafkamocker_topic{ name = <<"events">>,
                           partitions =
                           [#kafkamocker_partition{ id = 0, leader = 1,
                                        message_sets =
                                        [#kafkamocker_message_set{ size = 1,
                                                       messages =
                                                       [#kafkamocker_message{ value = <<"foo">>}]}]}]}]}.

produce_reply()->
    Packet =
        <<0,0,0,1,0,0,0,1,0,6,101,118,101,110,116,115,0,0,0,1,0,0,0,0,0,0,0,0,0,0>>,
    PacketSize = byte_size(Packet),
    Reply  = <<PacketSize:32, Packet/binary>>,
    Reply.

metadata_reply()->
    Packet =
  <<0,0,0,1,                           %% request id
    0,0,0,1,                           %% number of brokers,
    0,0,0,1,                           %% broker 1
    0,9,                               %% length of broker host
    108,111,99,97,108,104,111,115,116, %% "localhost"
    0,0,35,131,                        %% 9091
    0,0,0,1,                           %% how many topics
    0,0,0,6,                           %% topic 1
    101,118,101,110,116,115,           %% "events"
    0,0,0,1,                           %% how many partitions
    0,0,                               %% partition 1 error code
    0,0,0,0,                           %% partition 1 id
    0,0,0,1,                           %% partition 1 leader
    0,0,0,1,                           %% how many replicas
    0,0,0,1,                           %% replica 1
    0,0,0,1,                           %% how many isr
    0,0,0,1                            %% isr 1
   >>,
    PacketSize = byte_size(Packet),
    Reply  = <<PacketSize:32, Packet/binary>>,
    Reply.
encode(CorrelationId, Bin)->
    Packet = <<CorrelationId:32, Bin/binary>>,
    PacketSize = byte_size(Packet),
    Reply  = <<PacketSize:32,Packet/binary>>,
    Reply.

encode(#kafkamocker_metadata{brokers = Brokers, topics = Topics }=_Packet)->
    CorrelationId = 1,
    BrokersEncoded = encode_brokers(Brokers),
    TopicsEncoded = encode_topics(Topics),
    encode(CorrelationId, <<BrokersEncoded/binary, TopicsEncoded/binary>>);
encode(#kafkamocker_produce_request{ cor_id = CorId, required_acks = _ReqAck, timeout = _Timeout, topics = Topics })->
    {EncodedProduceResponse,Offset} = encode_produce_response_topics(Topics),
    encode(CorId, <<EncodedProduceResponse/binary, Offset:(8*8)>>).

encode_produce_response_topics(Topics)->
    encode_produce_response_topics(Topics,<<>>,0).
encode_produce_response_topics([Topic|Rest],Bin,Ctr)->
    encode_produce_response_topics(Rest, <<Bin/binary,(encode_produce_response_topic(Topic))/binary>>, Ctr+1);
encode_produce_response_topics(_,Bin,Ctr) ->
    Offset = 0, %length(M),
    {<<Ctr:32,Bin/binary>>, Offset}.

encode_produce_response_topic(#kafkamocker_topic{ name = Name, partitions = Partitions })->
    EncodedPartitionResponse = encode_produce_response_partitions(Partitions),
    NameSize = byte_size(Name),
    <<NameSize:16, Name/binary, EncodedPartitionResponse/binary>>.

encode_produce_response_partitions(Partitions)->
    encode_produce_response_partitions(Partitions,<<>>,0).
encode_produce_response_partitions([Partition|Rest],Bin,Ctr)->
    encode_produce_response_partitions(Rest, <<Bin/binary,(encode_produce_response_partition(Partition))/binary>>, Ctr+1);
encode_produce_response_partitions(_,Bin,Ctr) ->
    <<Ctr:32,Bin/binary>>.

encode_produce_response_partition(_)->
    ErrorCode = 0,
    <<ErrorCode:16>>.

encode_brokers(L) ->
    encode_brokers(L,<<>>,0).
encode_brokers([Broker|Rest],Bin,Ctr)->
    encode_brokers(Rest, <<Bin/binary,(encode_broker(Broker))/binary>>, Ctr+1);
encode_brokers(_,Bin,Ctr) ->
    <<Ctr:32,Bin/binary>>.

encode_topics(L) ->
    encode_topics(L,<<>>,0).
encode_topics([Topic|Rest],Bin,Ctr)->
    encode_topics(Rest, <<Bin/binary,(encode_topic(Topic))/binary>>, Ctr+1);
encode_topics(_,Bin,Ctr) ->
    <<Ctr:32,Bin/binary>>.

encode_broker(#kafkamocker_broker{id = Id, host = Host, port = Port } = _Packet)->
    HostBin = to_binary(Host),
    HostSize = byte_size(HostBin),
    PortBin = port_to_binary(Port),
    <<Id:32, HostSize:16, HostBin/binary, PortBin/binary>>.

encode_topic(#kafkamocker_topic{name = Name, partitions = Partitions }= _Packet)->
    NameSize = byte_size(Name),
    Encoded = encode_partitions(Partitions),
    <<NameSize:32, Name/binary, Encoded/binary>>.

encode_partitions(L) ->
    encode_partitions(L,<<>>,0).
encode_partitions([Partition|Rest],Bin,Ctr)->
    encode_partitions(Rest, <<Bin/binary,(encode_partition(Partition))/binary>>, Ctr+1);
encode_partitions(_,Bin,Ctr) ->
    <<Ctr:32,Bin/binary>>.

encode_partition(#kafkamocker_partition{error_code = Error, id = Id, leader = Leader, replicas = Replicas, isrs = Isrs }= _Packet)->
    FinalError = case Error of undefined -> 0;_ -> Error end,
    EncodedReplicas = encode_replicas(Replicas),
    EncodedIsrs = encode_isrs(Isrs),
    <<FinalError:16, Id:32, Leader:32, EncodedReplicas/binary, EncodedIsrs/binary>>.

encode_replicas(L) ->
    encode_replicas(L,<<>>,0).
encode_replicas([Replica|Rest],Bin,Ctr)->
    encode_replicas(Rest, <<Bin/binary,(encode_replica(Replica))/binary>>, Ctr+1);
encode_replicas(_,Bin,Ctr) ->
    <<Ctr:32,Bin/binary>>.

encode_isrs(L) ->
    encode_isrs(L,<<>>,0).
encode_isrs([Isr|Rest],Bin,Ctr)->
    encode_isrs(Rest, <<Bin/binary,(encode_isr(Isr))/binary>>, Ctr+1);
encode_isrs(_,Bin,Ctr) ->
    <<Ctr:32,Bin/binary>>.

encode_replica(#kafkamocker_replica{ id = Id })->
    <<Id:32>>.
encode_isr(#kafkamocker_isr{ id = Id })->
    <<Id:32>>.

port_to_binary(N)->
    <<_X:16,R/binary>> = term_to_binary(N) , R.

%% decode produce
% <<0,0,0,76,
% 0,0, % <<0, 0,
% 0,0, % _ApiVersion:16, Rest/binary>> ->
% 0,0,0,2, % cid
% 0,4,     % clientidlen
% 101,107,97,102, %clientid
% 0,0,     % reqacks no
% 0,0,0,100, %timeout
% 0,0,0,1, % 1 topic
% 0,6
decode_produce(Packet)->
    case Packet of
        <<CorrelationId:32, ClientIdLen:16, ClientId:ClientIdLen/binary, RequireAcks:16, Timeout:32, Rest/binary >> ->
            debug("~n asked to decode topics ~p",[Rest]),
            {Topics, Remaining } = decode_to_topics(Rest),
            { #kafkamocker_produce_request{ cor_id = CorrelationId, client_id = ClientId, required_acks = RequireAcks, timeout = Timeout, topics = Topics}, Remaining};
        _ ->
            {
          #kafkamocker_produce_request{},
          Packet
         }
    end.

decode_to_topics(Packet)->
    case Packet of
        <<Len:32,Rest/binary>> ->
            decode_to_topics(Len,Rest,[]);
        _E ->
            {[],Packet}
    end.
decode_to_topics(0, Packet, Previous)->
    {Previous, Packet};
decode_to_topics(Counter, Packet, Previous) ->
    {Next,Rest} = decode_to_topic(Packet),
    decode_to_topics(Counter-1, Rest, [Next|Previous]).

decode_to_topic(<<NameLen:16, Name:NameLen/binary,PartitionsBinary/binary>>)->
    {Partitions,Rest} = decode_to_partitions(PartitionsBinary,[]),
    {#kafkamocker_topic{ name = Name, partitions = Partitions},
     Rest};
decode_to_topic(Rest)->
    {#kafkamocker_topic{},Rest}.

decode_to_partitions(<<>>, Previous) ->
    {Previous, <<>>};
decode_to_partitions(<<_Len:32, Id:32, ByteSize:32, MessageSetsEncoded:ByteSize/binary, Remaining/binary>>, Previous) ->
    {MessageSets,_Rest} = decode_message_set(MessageSetsEncoded, []),
    decode_to_partitions(Remaining, [#kafkamocker_partition{ id = Id, message_sets_size = length(MessageSets), message_sets = MessageSets } | Previous ]);
decode_to_partitions(Rest, Previous) ->
    debug("~n dont know what to do with rest:~p previous:~p",[Rest, Previous]),
    {Previous,Rest}.



% decode_to_message_sets(MessageSets)->
%     decode_to_message_sets(MessageSets,[]).
decode_message_set(<<>>, Previous)->
    {[#kafkamocker_message_set{ messages = lists:reverse(Previous), size = length(Previous)  }],<<>>};
decode_message_set(<<_Offset:64, Size:32, Messages:Size/binary,Rest/binary>>, Previous) ->
    {Next,_} = decode_to_message(Messages),
    %debug("~n ~p rest is ~p, prev is ~p",[Next,Rest,Previous]),
    decode_message_set(Rest, [Next| Previous ]);
decode_message_set(Rest,Previous)->
    debug("~n cant figure out ~p",[Rest]),
    {Previous,Rest}.

decode_to_messages(MessagesEncoded)->
    decode_to_messages(MessagesEncoded,[]).

decode_to_messages(<<>>, Previous)->
    {Previous,<<>>};
decode_to_messages(Packet, Previous) ->
    {Next,Rest} = decode_to_message(Packet),
    {[Next|Previous], Rest}.

decode_to_message(<<_CRC:32, _Magic:8, Atts:8, KeyLen:32, Key:KeyLen/binary, ByteSize:32, Value:ByteSize/binary, Rest/binary >>)->
    {#kafkamocker_message{ attributes = Atts, key = Key, value = Value },Rest};
decode_to_message(<<_CRC:32, _Magic:8, Atts:8, 255, 255, 255, 255, ValueLen:32, Value:ValueLen/binary, Rest/binary >>)->
    {#kafkamocker_message{ attributes = Atts, value = Value },Rest};
decode_to_message(Rest)->
    {#kafkamocker_message{},Rest}.

produce_request_to_messages(#kafkamocker_produce_request{ topics = Topics}) ->
    lists:reverse(produce_topics_to_messages(Topics,[])).

produce_topics_to_messages([], Acc)->
    Acc;
produce_topics_to_messages([#kafkamocker_topic{ partitions = Partitions} | Topics], Acc)->
    Next = produce_partitions_to_messages(Partitions, Acc),
    produce_topics_to_messages(Topics, Next).

produce_partitions_to_messages([],Acc)->
    Acc;
produce_partitions_to_messages([#kafkamocker_partition{ message_sets = MessageSets} | Partitions], Acc)->
    Next = produce_messagesets_to_messages(MessageSets, Acc),
    produce_partitions_to_messages(Partitions, Next).

produce_messagesets_to_messages([],Acc)->
    Acc;
produce_messagesets_to_messages([#kafkamocker_message_set{ messages = Messages} | MessageSets], Acc)->
    Next = produce_messages_to_values(Messages, Acc),
    produce_messagesets_to_messages(MessageSets, Next).
produce_messages_to_values([], Acc)->
    Acc;
produce_messages_to_values([#kafkamocker_message{ value = Value} | Messages], Acc)->
    Next = [Value|Acc],
    produce_messages_to_values(Messages, Next).

to_binary(Bin) when is_binary(Bin)->
    Bin;
to_binary(Str) ->
    list_to_binary(Str).
