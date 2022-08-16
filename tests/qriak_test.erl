-module(qriak_test).
-behavior(riak_test).
-include_lib("eunit/include/eunit.hrl").

% BEHAVIOUR EXPORTS
-export([confirm/0]).

% TEST EXPORTS
-export([
    between_test/1,
    is_test/1,
    none_test/1,
    aggregation_test/1
]).
-compile([export_all, nowarn_export_all]).
-define(QRIAK_BUCKET, <<"qriak_bucket">>).
-define(TESTS, [
    between_test,
    is_test,
    none_test,
    aggregation_test
]).
-define(RESULT(Code, Msg, Body),
    {ok, {{_Version, Code, Msg}, _Headers, Body}}).


% BEHAVIOUR EXPORTS
confirm() ->
    try
        ok = application:ensure_started(inets),
        Nodes = rt:build_cluster(3),

        ok = load_test_data(Nodes),
    
        lists:foreach(
            fun(Test) ->
                lager:info("Running test ~p", [Test]),
                ?MODULE:Test(Nodes)
            end,
            ?TESTS),
        
        pass
    catch
        Reason ->
            lager:info("Failed test ~p with reason ~p",[?MODULE, Reason]),
            fail
    end.

% TEST EXPORTS
between_test(_Nodes) ->
    http_qriak_query(
        "id BETWEEN '1' '4'", 
        [[{<<"id">>, [<<"1">>]}],
        [{<<"id">>, [<<"2">>]}],
        [{<<"id">>, [<<"3">>]}],
        [{<<"id">>, [<<"4">>]}]]).

is_test(_Nodes) ->
    http_qriak_query(
        "id IS '1'", 
        [[{<<"id">>, [<<"1">>]}]]).

none_test(_Nodes) ->
    http_qriak_query(
        "id IS 'not_an_id'",
        []).

aggregation_test(_Nodes) ->
    http_qriak_query(
        "id BETWEEN '1' '2' AND second_field BETWEEN '1' '2001'",
        [[{<<"id">>, [<<"1">>]}, {<<"second_field">>, [<<"1000">>]}],
        [{<<"id">>, [<<"2">>]}, {<<"second_field">>, [<<"2000">>]}]]),
    http_qriak_query(
        "id IS '1' OR second_field BETWEEN '2001' '4000'",
        [[{<<"id">>, [<<"1">>]}, {<<"second_field">>, [<<"1000">>]}],
        [{<<"id">>, [<<"3">>]}, {<<"second_field">>, [<<"3000">>]}],
        [{<<"id">>, [<<"4">>]}, {<<"second_field">>, [<<"4000">>]}]]
    ).



http_qriak_query(Where, ExpectedItems) ->
    WhereParts = string:replace(Where, " ", "%20", all),
    Where1 = lists:flatten(WhereParts),
    URL = "http://localhost:10018/qriak?query=FROM%20"
        ++ erlang:binary_to_list(?QRIAK_BUCKET)
        ++ "%20WHERE%20"
        ++ Where1,
    ?RESULT(200, "OK", Body) = httpc:request(get, {URL, []}, [], []),
    ResultCount = erlang:length(ExpectedItems),
    #{info := #{total_count := ResultCount}, items := Items} =
        parse_body(Body),
    SortedExpectedItems = lists:sort(ExpectedItems),
    SortedItems = lists:sort(Items),
    ?assertEqual(SortedExpectedItems, SortedItems).
    
parse_body(Body) ->
    {struct, Resp} = mochijson2:decode(Body),
    {struct, Result} = proplists:get_value(<<"result">>, Resp),
    {struct, Info} = proplists:get_value(<<"info">>, Result),
    TotalCount = proplists:get_value(<<"total_count">>, Info),
    Items = proplists:get_value(<<"items">>, Result),
    Items1 = lists:map(
        fun({struct, It}) -> It end, Items),
    #{info => #{total_count => TotalCount}, items => Items1}.

    
    

% TEST DATA LOADING FUNCTIONS
load_test_data([Node | _Nodes]) ->
    lager:info("Filling QRIAK_BUCKET (~p)", [?QRIAK_BUCKET]),
    Client = rt:pbc(Node),
    
    CheckFun =
        fun
            ({error, _ErrorTerm}) ->
                error;
            ({ok, Result}) ->
                {ok, Result}
        end,

    Objects = check(CheckFun, generate_objects(), generate_objects_error),
    check(CheckFun, load_objects(Client, Objects), load_objects_error).

generate_objects() ->
    Items =
        [#{id => <<"1">>, first_field => <<"aaabbbccc">>, second_field => <<"1000">>},
        #{id => <<"2">>, first_field => <<"aaaxxxyyy">>, second_field => <<"2000">>},
        #{id => <<"3">>, first_field => <<"pppqqqttt">>, second_field => <<"3000">>},
        #{id => <<"4">>, first_field => <<"zzzxxxzzz">>, second_field => <<"4000">>}],
    {ok, lists:map(
        fun(#{id := Id, first_field := FirstField, second_field := SecondField} = Item) ->
            Object = riakc_obj:new(?QRIAK_BUCKET, Id, Item),
            Metadata1 = riakc_obj:get_update_metadata(Object),
            Metadata2 = riakc_obj:set_secondary_index(
                Metadata1,
                [
                    {{binary_index, "id"}, [Id]},
                    {{binary_index, "first_field"}, [FirstField]},
                    {{binary_index, "second_field"}, [SecondField]} 
                ]),
            riakc_obj:update_metadata(Object, Metadata2)
        end,
        Items)}.

load_objects(Client, Objects) ->
    {ok, lists:foreach(
        fun(Object) ->
            riakc_pb_socket:put(Client, Object)
        end,
        Objects)}.

% UTIL FUNCTIONS
check(CheckFun, Input, Type) ->
    try
        {ok, Result} = CheckFun(Input),
        Result
    catch 
        Class:Error ->
            throw({error, Type, Class, Error})
    end.
