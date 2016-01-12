-module(ecc_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    Core = {ecc_core, {ecc_core, start_link, []},
              permanent, 2000, worker, [ecc_core]},
    Children = [Core],
    RestartStrategy = {one_for_one, 0, 1},
    {ok, {RestartStrategy, Children}}.