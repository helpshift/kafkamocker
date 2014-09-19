-record(kafkamocker_fsm, {brokers=[]::list(), topics=[]::list(), callback, metadata, conns=[]::list()}).
-record(kafkamocker_metadata, {brokers, topics}).
-record(kafkamocker_broker, { id, host, port }).
-record(kafkamocker_topic, { name, error_code, partitions }).
-record(kafkamocker_partition, { error_code, id, leader, replicas, isrs, message_sets, message_sets_size}).
-record(kafkamocker_replica, { id }).
-record(kafkamocker_isr, { id }).

-type kafkamocker_kafka_byte() :: <<_:8>>.

-record(kafkamocker_message_set, {offset=0::integer(), size=0::integer(), messages=[]::list()}).
-record(kafkamocker_message, {crc=0::integer(), magicbyte=0::kafkamocker_kafka_byte(), attributes=0::kafkamocker_kafka_byte(), key = undefined::binary(), value::binary()}).



-record(kafkamocker_produce_request, { cor_id, client_id, error_code, required_acks, timeout, topics}).

-define(KAFKAMOCKER_METADATA_REQUEST, <<0,3>>).
-define(KAFKAMOCKER_SAMPLE_METADATA,
        #kafkamocker_metadata{
          brokers = [#broker{ id = 1, host = <<"localhost">>, port = 9091}],
          topics = [#topic{ name = <<"events">>, error_code = 0,
                            partitions = [#partition { error_code = 0, id = 0, leader = 1,
                                                       replicas = [#replica{ id = 1 }],
                                                       isrs = [#isr{ id = 1 }]
                                                      }
                                         ]
                           }
                   ]
         }).
