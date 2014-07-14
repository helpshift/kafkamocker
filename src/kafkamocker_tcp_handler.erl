%% Feel free to use, reuse and abuse the code in this file.
-module(kafkamocker_tcp_handler).
-behaviour(ranch_protocol).
-include("kafkamocker.hrl").
-export([start_link/4]).
-export([init/4]).

-record(state, { callback, metadata, length = 0, previous = 0, buffer = <<>> }).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, [Callback, Metadata] = _Opts) ->
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, #state{ callback = Callback, metadata = Metadata } ).

loop(Socket, Transport, State) ->
    case Transport:recv(Socket, 0, 5000) of
        {error,ebadf}->
            loop(Socket, Transport, State);

        {ok, <<Len:32, Packet:Len/binary, Rest/binary>> = Full} when State#state.buffer =:= <<>> ->
            io:format("~n got1 take only ~p of ~p ~p rest:~p",[Len,byte_size(Full)-4,Packet,Rest]),
            handle_loop_done(Socket, Transport, State#state{ length = Len,
                                                             buffer = << >> % Rest
                                                             }, Packet);
            % Curr =(Len - byte_size(Rest)  - 4 - 2 - 2 ),
            % case Curr of
            %     Less when Less < 1  ->
            %         io:format("got ~p which is < 1",[Less]),
            %         handle_loop_done(Socket, Transport, State, Rest);
            %     _Size ->
            %         io:format("got ~p which is > 1, so add to ~p",[_Size, byte_size(State#state.buffer)]),
            %         loop(Socket, Transport, State#state{ length = Len - 4, buffer = <<(State#state.buffer)/binary, Rest/binary  >> } )
            % end;
        {error,closed} when State#state.buffer =/= <<>> ->
            io:format("~n closed , handle ",[]),
            handle_loop_done(Socket, Transport, State, State#state.buffer);

        {ok, <<Rest/binary>>} when State#state.buffer =/= <<>> ->
            io:format("~n got2 ~p",[Rest]),
            Len = State#state.length,
            Final =  <<(State#state.buffer)/binary, Rest/binary  >>,
            Next = byte_size(Final),
            case (Len - Next) of
                Less  when Less < 1  ->
                    io:format("got ~p which is < 1",[Less]),
                    handle_loop_done(Socket, Transport, State, Final);
                _Size ->
                    loop(Socket, Transport, State#state{ length = Len - 4, buffer = Final, previous = Next } )
            end;

        {error,timeout} when State#state.buffer =/= <<>> ->
            handle_loop_done(Socket, Transport, State#state{buffer = <<>> }, State#state.buffer);
        {error,_E} when State#state.buffer =/= <<>> ->
            io:format("~n huh error ~p",[_E]),
            % huh when you have a buffer
            ok;
        {error, _E} ->
            loop(Socket, Transport, State);
        Data ->
            error_logger:info_msg("Not implemented ~p, bufflength is ~p",[ Data, State#state.length]),
            loop(Socket, Transport, State)
    end.

handle_loop_done(Socket, Transport, #state{ callback = Callback, metadata = Metadata} = State, Packet)->
    io:format("~nhandle_loop_done got ~p",[Packet]),
    case Packet of
        %%
        %% METADATA request
        %%
        <<0, 3, _ApiVersion:16, _CorrelationId:32, ClientIdLen:16, _ClientId:ClientIdLen/binary, _Rest/binary>> ->
            Reply = kafkamocker:encode(Metadata),
            Transport:send(Socket,Reply);

        %%
        %% PRODUCE request
        %%
        <<0, 0, _ApiVersion:16, Rest/binary>> ->
            Produced = kafkamocker:decode_produce(Rest),
            io:format("~n produced : ~p",[Produced]),
            Ack = Produced#produce_request.required_acks,
            case Ack of
                0 ->
                    % sync, no reply
                    ok;
                _ ->
                    Reply =  kafkamocker:encode(Produced),
                    Transport:send(Socket, Reply)
            end,
            Callback ! {produce, kafkamocker:produce_request_to_messages(Produced)},
            loop(Socket, Transport, State)
    end,
    error_logger:info_msg("buffer size is now ~p",[byte_size(State#state.buffer)]),
    loop(Socket, Transport, State).
