## kafkamocker
kafkamocker is an erlang/OTP app that simulates a kafka 0.8 broker, it is able to:

* accept and reply to metadata requests
* accept async produce requests
* accept and reply to sync produce requests
* allow you to inspect what a consumer would get

## Purpose
To embed within kafka producers and consumers without needing an actual kafka broker running anywhere.

## Usage

Add this line to your rebar.config, and `rebar get-deps`

    {kafkamocker, ".*", {git, "https://github.com/helpshift/kafkamocker"}}

Use this within your kafka producer eunit test as follows:

```erlang

    kafka_test_() ->
        {setup,
         fun() ->
                 %% kafkamocker uses ranch to listen as a tcp server
                 application:start(ranch),

                 %% let's have a consumer with any logic you want to check what kafkamocker got
                 Pid = spawn( fun() -> kafka_consumer_loop([],{self(),0}) end),
                 erlang:register(kafka_consumer, Pid),
                 [application:load(X) ||X<- [kafkamocker] ],

                 %% lets simulate a kafka broker, by making kafkamocker listen on 9091
                 application:set_env(kafkamocker, kafkamocker_callback, kafka_consumer),
                 application:set_env(kafkamocker, kafkamocker_bootstrap_broker, {"localhost",9091}),

                 [ application:start(X) || X<- [ranch, kafkamocker]],

                 %% you can embed kafkamocker within your app, and it will not start any workers
                 %% start_link will read the configurations we set and start kafkamocker
                 kafkamocker_fsm:start_link()
         end,
         fun(_) ->
                 kafka_consumer ! stop,
                 erlang:unregister(kafka_consumer),
                 [ application:stop(X) || X<- lists:reverse([ranch, kafkamocker]) ],
                 ok
         end,
         [
           %% your tests
         ]}.

    kafka_consumer_loop(Acc,Meta)->
        receive
           {produce, X} ->
                ?debugFmt("incoming: ~p",[X]
                kafka_consumer_loop(Acc++X, Meta);
           _
                kafka_consumer_loop(Acc, Meta)
        end.

```

## License

```
Copyright 2014, Helpshift, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
