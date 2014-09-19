%%%-------------------------------------------------------------------
%%% File    : kafkamocker_fsm.erl
%%% Description :
%%%
%%%-------------------------------------------------------------------
-module(kafkamocker_fsm).

-behaviour(gen_fsm).
%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------
-include("kafkamocker.hrl").
%-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% External exports
-export([start_link/0, start_link/1]).

%% gen_fsm callbacks
-export([init/1, state_name/2, state_name/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-define(SERVER, ?MODULE).

%%====================================================================
%% External functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link/0
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link()->
    start_link({metadata,kafkamocker:metadata()}).
start_link({broker, Broker, Topics}) ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, {broker, Broker, Topics}, []);
start_link({metadata,#kafkamocker_metadata{ brokers = Brokers, topics = Topics }  = Metadata}) ->
    case gen_fsm:start_link({local, ?SERVER}, ?MODULE, [Metadata, Topics], []) of
        {ok,Pid}->
            [ begin
                  gen_fsm:send_event(Pid, {broker, start, Broker})
              end || Broker <- Brokers ],
            {ok,Pid};
        _E ->
            _E
    end.

%%====================================================================
%% Server functions
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, StateName, StateData}          |
%%          {ok, StateName, StateData, Timeout} |
%%          ignore                              |
%%          {stop, StopReason}
%%--------------------------------------------------------------------
init([Metadata, Topics]) ->
    GotCallbackAs = application:get_env(kafkamocker, kafkamocker_callback),
    Callback = case GotCallbackAs of
                   {ok, PidName}->
                       PidName;
                   _ ->
                       erlang:register( kafkamocker_debug_loop, spawn(fun()-> debug_loop() end)),
                       kafkamocker_debug_loop
               end,
    {ok,state_name, #kafkamocker_fsm{ callback = Callback, metadata = Metadata, topics = Topics}}.

%%--------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
state_name({broker, start, #kafkamocker_broker{ id = BrokerId, host = BrokerHost, port = BrokerPort} = Broker}, #kafkamocker_fsm{ brokers = Brokers, callback = Callback, metadata = Metadata } = StateData) ->
    start_broker(BrokerHost, BrokerPort, BrokerId, Callback, Metadata),
    {next_state, state_name, StateData#kafkamocker_fsm{ brokers = [Broker|Brokers] }};
state_name({broker, stop, #kafkamocker_broker{ host = BrokerHost, port = BrokerPort}}, StateData)->
    BrokerName = {BrokerHost,BrokerPort},
    _Stopped = ranch:stop_listener(BrokerName),
    %?debugFmt("~n stop ~p => ~p",[Broker, _Stopped]),
    {next_state, state_name, StateData};
state_name(simulate_brokers_down, #kafkamocker_fsm{ brokers = Brokers } = StateData)->
    [ begin
          stop_broker(BrokerHost,BrokerPort)
          %,io:format("~n stop ~p:~p => ~p",[BrokerHost,BrokerPort, Stopped])
          end || #kafkamocker_broker{ host = BrokerHost, port = BrokerPort }  <- Brokers ],
    {next_state, state_name, StateData};
state_name(_UnknownEvent, StateData) ->
    io:format("dont know how to handle ~p",[_UnknownEvent]),
    {next_state, state_name, StateData}.

%%--------------------------------------------------------------------
%% Func: StateName/3
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%--------------------------------------------------------------------
state_name({metadata, #kafkamocker_metadata{ brokers = Brokers} = Metadata}, _From, #kafkamocker_fsm{ callback = Callback } = State)->
      [ begin
            start_broker(BrokerHost, BrokerPort, BrokerId, Callback, Metadata)
        end || #kafkamocker_broker{ host = BrokerHost, port = BrokerPort, id = BrokerId} <- Brokers],
    {reply, ok, state_name, State#kafkamocker_fsm{ metadata = Metadata }};
state_name({get, metadata}, _From, StateData) ->
    Reply = StateData#kafkamocker_fsm.metadata,
    {reply, Reply, state_name, StateData};
state_name(Event, _From, StateData) ->
    Reply = Event,
    {reply, Reply, StateData}.

%%--------------------------------------------------------------------
%% Func: handle_event/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
handle_event(_Event, StateName, StateData) ->
    {next_state, StateName, StateData}.

%%--------------------------------------------------------------------
%% Func: handle_sync_event/4
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%--------------------------------------------------------------------
handle_sync_event(Event, _From, StateName, StateData) ->
    Reply = Event,
    {reply, Reply, StateName, StateData}.

%%--------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
handle_info(_Info, StateName, StateData) ->
    {next_state, StateName, StateData}.

%%--------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _StatData) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change/4
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState, NewStateData}
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
debug_loop()->
    receive
        X ->
            error_logger:info_msg("consumed: ~p",[X]),
            debug_loop()
    end.

start_broker(BrokerHost, BrokerPort, BrokerId, Callback, Metadata)->
    BrokerName = {BrokerHost,BrokerPort},
    _Stopped = stop_broker(BrokerHost, BrokerPort),
    _Started = ranch:start_listener(BrokerName, BrokerId,
                                    ranch_tcp, [{port, BrokerPort}], kafkamocker_tcp_handler, [Callback, Metadata]),
    io:format("~n ~p => stopped:~p started:~p",[BrokerId, _Stopped, _Started]),
    _Started.

stop_broker(BrokerHost, BrokerPort)->
    ranch:stop_listener({BrokerHost,BrokerPort}).
