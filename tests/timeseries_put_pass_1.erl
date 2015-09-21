-module(timeseries_put_pass_1).

-behavior(riak_test).

-export([
	 confirm/0
	]).

-import(timeseries_util, [
			  get_ddl/1,
			  confirm_put/3
			  ]).

confirm() ->
    Cluster = single,
    DDL = get_ddl(docs),
    Expected = {ok, "GeoCheckin created\n\nWARNING: After activating GeoCheckin, nodes in this cluster\ncan no longer be downgraded to a version of Riak prior to 2.0\n"},
        confirm_put(Cluster, DDL, Expected).
