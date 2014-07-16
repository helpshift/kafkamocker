-module(kafkamocker_tests).
-ifdef(TEST).
-include("kafkamocker.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/qlc.hrl").

metadata_test_() ->
    {setup,
     fun() ->
             ok
     end,
     fun(_) ->
             ok
     end,
     [
      {spawn, ?_test(?debugVal(t_metadata_encode_test()))}
     ]}.

produce_test_() ->
    {setup,
     fun() ->
             ok
     end,
     fun(_) ->
             ok
     end,
     [
      {spawn, ?_test(?debugVal(t_produce_encode_test()))}
     ]}.

t_metadata_encode_test()->
    ?assertEqual(  kafkamocker:metadata_reply() ,
                   kafkamocker:encode( kafkamocker:metadata())).

t_produce_encode_test()->
    ?assertEqual(  kafkamocker:produce_reply() ,
                   kafkamocker:encode( kafkamocker:produce())).


-endif.
