-module(fox_test_utils).

-export([rabbit_params/0]).

-include("fox.hrl").


-spec rabbit_params() -> #amqp_params_network{}.
rabbit_params() ->
    fox_utils:map_to_params_network(#{
        host => os:getenv("FOX_RABBIT_HOST", "localhost"),
        port => list_to_integer(os:getenv("FOX_RABBIT_PORT", "5672")),
        virtual_host => unicode:characters_to_binary(
            os:getenv("FOX_RABBIT_VHOST", "local-vhost")),
        username => unicode:characters_to_binary(
            os:getenv("FOX_RABBIT_USER", "guest")),
        password => unicode:characters_to_binary(
            os:getenv("FOX_RABBIT_PASSWORD", "guest"))
    }).
