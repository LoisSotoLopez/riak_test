%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2016, Russell Brown
%%% @doc
%%% Smallest, simplest test, just to get started
%%% @end
%%% Created :  3 May 2016 by Russell Brown <russelldb@basho.com>

-module(bigset_basic_test).

-export([confirm/0]).

-define(SET, <<"test_set">>).

-include_lib("eunit/include/eunit.hrl").

confirm() ->
    lager:info("Testing handoff"),

    lager:info("Start cluster"),


    Config = [{riak_core, [ {ring_creation_size, 16},
                            {vnode_management_timer, 1000} ]}],

    [N1, N2]=Nodes = rt:deploy_nodes(2, Config, [bigset]),
    rt:join_cluster(Nodes),
    N1Client = bigset_client:new(N1),
    N2Client = bigset_client:new(N2),

    %% add some data
    ok = bigset_client:update(?SET, [<<"test1">>], N1Client),
    ok = bigset_client:update(?SET, [<<"test2">>], N2Client),

    {ok, {ctx, <<>>}, {elems, E1}} = bigset_client:read(?SET, [], N1Client),
    {ok, {ctx, <<>>}, {elems, E2}} = bigset_client:read(?SET, [], N2Client),

    ?assertEqual(E1, E2),

    %% remove some data
    ok = bigset_client:update(?SET, [], [hd(E1)], [], N1Client),

    {ok, {ctx, <<>>}, {elems, E3}} = bigset_client:read(?SET, [], N2Client),

    ?assertEqual(tl(E1), E3),

    %% partition the cluster

    %% add and remove from one side only

    %% wait for hand-off

    %% check the remove is reflected on the hand-off target
    %% @TODO(HOW!!!!)  create only 2 nodes, partition them, update one
    %% side, heal, wait for handoff, re-partition, read the side that
    %% was not written too?

    pass.
