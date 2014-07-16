-module(kafkamocker_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_one, 5, 10}, []} }.

start_child(Module,ChildSpec) when is_tuple(ChildSpec) ->
    supervisor:start_child(Module,ChildSpec);

start_child(Module,InitArgs) ->
    case Module:get_child_spec(InitArgs) of
        [] ->
            ok;
        [ChildSpec] ->
            start_child(Module,ChildSpec);
        _E ->
            error_logger:info_msg("~n ~p unexp when start_child. got ~p",[Module,_E]),
            error
    end.
