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
start_link({metadata,#metadata{ brokers = Brokers, topics = Topics }  = Metadata}) ->
    case gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []) of
        {ok,Pid}->
            gen_fsm:sync_send_event(Pid, {set, metadata, Metadata}),
            [ begin
                  gen_fsm:send_event(Pid, {broker, Broker, Topics })
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
init(_Args) ->
    GotCallbackAs = application:get_env(kafkamocker, kafkamocker_callback),
    Callback = case GotCallbackAs of
                   {ok, Pid}->
                       Pid;
                   _ ->
                       erlang:register( kafkamocker_debug_loop, spawn(fun()-> debug_loop() end)),
                       kafkamocker_debug_loop
               end,
    {ok,state_name, #kafkamocker_fsm{ callback = Callback}}.

%%--------------------------------------------------------------------
%% Func: StateName/2
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%%--------------------------------------------------------------------
state_name({broker, #broker{ host = BrokerHost, port = BrokerPort} = Broker, Topics}, StateData) ->
    {ok, _} = ranch:start_listener(BrokerHost, 1,
                                   ranch_tcp, [{port, BrokerPort}], kafkamocker_tcp_handler, [StateData#kafkamocker_fsm.callback, StateData#kafkamocker_fsm.metadata  ]),
    {next_state, state_name, #kafkamocker_fsm{ broker = Broker, topics = Topics }}.

%%--------------------------------------------------------------------
%% Func: StateName/3
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%%--------------------------------------------------------------------
state_name({set, metadata, Metadata}, _From, StateData) ->
    Reply = ok,
    {reply, Reply, state_name, StateData#kafkamocker_fsm{ metadata = Metadata} };
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
            error_logger:info_msg("~n consumer got ~p",[X]),
            debug_loop()
    end.
