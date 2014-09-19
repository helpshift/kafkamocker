%% Feel free to use, reuse and abuse the code in this file.
-module(kafkamocker_tcp_handler).
-behaviour(ranch_protocol).
-include("kafkamocker.hrl").
-export([start_link/4]).
-export([init/4]).

-record(kafkamocker_state, { callback, metadata, length = 0, previous = 0, buffer = <<>> }).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, [Callback, Metadata] = _Opts) ->
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, #kafkamocker_state{ callback = Callback, metadata = Metadata } ).

loop(Socket, Transport, State) ->
    case Transport:recv(Socket, 0, 1000) of
        {error,ebadf}->
            loop(Socket, Transport, State);

        {ok, <<Len:32, Packet:Len/binary, Rest/binary>> = Full} when State#kafkamocker_state.buffer =:= <<>> ->
            kafkamocker:debug("~n got1 take only ~p of ~p ~p got1rest:~p",[Len,byte_size(Full)-4,Packet,Rest]),
            Next = handle_remaining(Socket, Transport, State#kafkamocker_state{ length = 0,
                                                             buffer = Rest
                                                             }, Full),
            loop(Socket, Transport, Next);
        {error,closed} when State#kafkamocker_state.buffer =/= <<>> ->
            kafkamocker:debug("closed when len:~p buffer:~p",[State#kafkamocker_state.length, byte_size(State#kafkamocker_state.buffer)]),
            Next = case State#kafkamocker_state.buffer of
                <<Len:32, Packet:Len/binary>> ->
                           TempNext = handle_remaining(Socket, Transport, State#kafkamocker_state{buffer = <<>> }, State#kafkamocker_state.buffer),
                           handle_loop_done(Socket, Transport, TempNext, Packet);
                Packet ->
                    handle_loop_done(Socket, Transport, State, Packet)
            end,
            loop(Socket, Transport, Next);

        {ok, <<Len:32,Rest/binary>>} when State#kafkamocker_state.buffer =:= <<>> ->
            kafkamocker:debug("supposed to get ~p but got only ~p so add to buffer",[Len,byte_size(Rest)]),
            Next = State#kafkamocker_state{ length =  Len, buffer = <<(State#kafkamocker_state.buffer)/binary, Rest/binary>> },
            loop(Socket, Transport, Next);

        {ok, <<Rest/binary>>} when State#kafkamocker_state.buffer =/= <<>> ->
            Combined =  <<(State#kafkamocker_state.buffer)/binary, Rest/binary  >>,
            Len = State#kafkamocker_state.length, %case State#kafkamocker_state.length of 0 -> RestLen; OldLen -> OldLen end,
            <<RestLen:32,_/binary>> = Rest,
            kafkamocker:debug("got2 ~p but got only ~p so add to buffer knownlen:~w, combined is ~w",[RestLen,byte_size(Rest), Len, byte_size(Combined)]),

            Next = case Combined of
                <<Final:Len/binary, Remaining/binary>> when Len =/= 0 ->
                    handle_loop_done(Socket, Transport, State#kafkamocker_state{length= 0, buffer = Remaining }, Final);
                <<RestLen:32, _Final:RestLen/binary, _Remaining/binary>> when RestLen =/= 0 ->
                    handle_remaining(Socket, Transport, State, Combined);
                _ ->
                   State#kafkamocker_state{ buffer = Combined }
            end,
            loop(Socket, Transport, Next);

        {error,timeout} when State#kafkamocker_state.buffer =/= <<>> ->
            loop(Socket, Transport, State);
        {error,_E} when State#kafkamocker_state.buffer =/= <<>> ->
            ok;
        {error, _E} ->
            loop(Socket, Transport, State);
        Data ->
            error_logger:info_msg("Not implemented buffer:~p~ndata:~p, bufflength is ~p",[ State#kafkamocker_state.buffer,Data, State#kafkamocker_state.length]),
            loop(Socket, Transport, State)
    end.


handle_remaining(Socket, Transport, State, Packet)->
    Packets = split_packet(Packet,[]),
    lists:foldl( fun(TempPacket, TempState)->
                         handle_loop_done(Socket, Transport, TempState, TempPacket)
                 end, State, Packets).

split_packet(<<>>,Acc)->
    lists:reverse(Acc);
split_packet(<<Len:32, Packet:Len/binary, Rest/binary>>, Acc)->
    split_packet(Rest, [Packet|Acc]).

handle_loop_done(Socket, Transport, #kafkamocker_state{ callback = Callback, metadata = Metadata} = State, Packet)->
    kafkamocker:debug("handle_loop_done with ~p",[byte_size(Packet)]),
    kafkamocker:debug("~nhandle_loop_done got ~p",[Packet]),
    Remaining =
        case Packet of
            %%
            %% METADATA request
            %%
            <<0, 3, _ApiVersion:16, _CorrelationId:32, ClientIdLen:16, _ClientId:ClientIdLen/binary, Rest/binary>> ->
                % Topics = kafkamocker:to_topics(Rest),
                % UnknownTopics = lists:foldl(fun(Topic, Prev) ->
                %                                     case find_topic_in_metadata(Topic,Metadata) of
                %                                         false ->
                %                                             [Topic|Prev];
                %                                         _ ->
                %                                             Prev
                %                                     end
                %                             end, [], Metadata#kafkamocker_metadata.topics),
                % UpdatedMetadata = add_new_topics_to_metadata(UnknownTopics, Metadata),
                Reply = kafkamocker:encode(Metadata),
                Transport:send(Socket,Reply),
                Rest;

            %%
            %% PRODUCE request
            %%
            <<0, 0, _ApiVersion:16, PacketToParse/binary>> ->
                {Produced, Rest} = kafkamocker:decode_produce(PacketToParse),
                kafkamocker:debug("~n produced : ~p",[Produced]),
                Ack = Produced#kafkamocker_produce_request.required_acks,
                case Ack of
                    0 ->
                                                % sync, no reply
                        ok;
                    _ ->
                        Reply =  kafkamocker:encode(Produced),
                        Transport:send(Socket, Reply)
                end,
                ProducedReply = kafkamocker:produce_request_to_messages(Produced),
                (catch Callback ! {produce, ProducedReply}),
                Rest
    end,
    kafkamocker:debug("  remaining ~p",[byte_size(Remaining)]),
    State#kafkamocker_state{ length = 0, buffer = <<>> } . %Remaining  }.
