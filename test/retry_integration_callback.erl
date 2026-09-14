-module(retry_integration_callback).
-behaviour(fox_subs_worker).

-export([init/2, handle/3, terminate/2]).

init(_Channel, Owner) ->
    {ok, Owner}.

handle(_Msg, _Channel, Owner) ->
    {ok, Owner}.

terminate(Channel, Owner) ->
    Owner ! {subscription_terminate, self(), Channel},
    ok.
