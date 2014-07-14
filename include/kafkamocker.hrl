-record(kafkamocker_fsm, {broker, topics, callback, metadata}).
-record(metadata, {brokers, topics}).
-record(broker, { id, host, port }).
-record(topic, { name, error_code, partitions }).
-record(partition, { error_code, id, leader, replicas, isrs, message_sets, message_sets_size}).
-record(replica, { id }).
-record(isr, { id }).

-type kafka_byte() :: <<_:8>>.

-record(message_set, {offset=0::integer(), size=0::integer(), messages=[]::list()}).
-record(message, {crc=0::integer(), magicbyte=0::kafka_byte(), attributes=0::kafka_byte(), key = undefined::binary(), value::binary()}).



-record(produce_request, { cor_id, client_id, error_code, required_acks, timeout, topics}).

-define(METADATA_REQUEST, <<0,3>>).
-define(SAMPLE_METADATA,
        #metadata{
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
