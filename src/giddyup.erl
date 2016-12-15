%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013-2016 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(giddyup).

-export([
    get_suite/1,
    post_result/1,
    post_all_artifacts/4,
    post_artifact/2]).
-define(STREAM_CHUNK_SIZE, 8192).
-include("rt.hrl").

-spec get_suite(string()) -> [{atom(), term()}].
get_suite(Platform) ->
    Schema = get_schema(Platform),
    Name = kvc:path('project.name', Schema),
    lager:info("Retrieved Project: ~s", [Name]),
    Tests = kvc:path('project.tests', Schema),
    TestProps  =
        fun(Test) ->
            [
                {id, kvc:path(id, Test)},
                {backend,
                 case kvc:path('tags.backend', Test) of
                     [] -> undefined;
                     X -> binary_to_atom(X, utf8)
                 end},
                {platform, list_to_binary(Platform)},
                {version, rt:get_version()},
                {project, Name}
            ] ++
            case kvc:path('tags.upgrade_version', Test) of
                [] -> [];
                UpgradeVsn -> [{upgrade_version, binary_to_atom(UpgradeVsn, utf8)}]
            end ++
            case kvc:path('tags.multi_config', Test) of
                [] -> [];
                MultiConfig -> [{multi_config, binary_to_atom(MultiConfig, utf8)}]
            end
        end,
    [ { binary_to_atom(kvc:path(name, Test), utf8), TestProps(Test) } || Test <- Tests].

get_schema(Platform) ->
    get_schema(Platform, 3).

get_schema(Platform, Retries) ->
    Host = rt_config:get(giddyup_host),
    Project = rt_config:get(rt_project),
    Version = rt:get_version(),
    URL = lists:flatten(io_lib:format("http://~s/projects/~s?platform=~s&version=~s", [Host, Project, Platform, Version])),
    lager:info("giddyup url: ~s", [URL]),

    rt:check_ibrowse(),
    case {Retries, ibrowse:send_req(URL, [], get, [], [])} of
        {_, {ok, "200", _Headers, JSON}} -> mochijson2:decode(JSON);
        {0, Error} ->
            lager:error("GiddyUp GET failed: ~p", [Error]),
            exit(1);
        {_, Error} ->
            lager:warning("GiddyUp GET failed: ~p", [Error]),
            lager:warning("GiddyUp trying ~p more times", [Retries]),
            timer:sleep(60000),
            get_schema(Platform, Retries - 1)
    end.

-spec post_result([{atom(), term()}]) -> {ok, string()} | error.
post_result(TestResult) ->
    Host = rt_config:get(giddyup_host),
    URL = "http://" ++ Host ++ "/test_results",
    lager:info("giddyup url: ~s", [URL]),
    rt:check_ibrowse(),
    case rt:post_result(TestResult, #rt_webhook{name="GiddyUp", url=URL, headers=[basic_auth()]}) of
        {ok, RC, Headers} ->
            {_, Location} = lists:keyfind("Location", 1, Headers),
            lager:info("Test Result successfully POSTed to GiddyUp! ResponseCode: ~s, URL: ~s", [RC, Location]),
            {ok, Location};
        error ->
            error
    end.

%% Store all generated logs in S3
post_all_artifacts(TestResult, Base, Log, CoverageFile) ->

    %% First initialize the tar file
    {Tar, TarFile} = create_tar_file(),

    %% Now push up the artifacts, starting with the test log
    post_artifact_and_add_to_tar(Base, Tar, {"riak_test.log", Log}),

    lists:foreach(fun({Name, Port}) ->
                      Contents = make_req_body(Port),
                      post_artifact_and_add_to_tar(Base, Tar, {Name, Contents})
                  end, rt:get_node_logs()),
    maybe_post_debug_logs(Base, Tar),
    lists:foreach(fun(CoverFile) ->
            Name = filename:basename(CoverFile) ++ ".gz",
            Contents = zlib:gzip(element(2, file:read_file(CoverFile))),
            post_artifact_and_add_to_tar(Base, Tar, {Name, Contents})
        end, [CoverageFile || CoverageFile /= cover_disabled]),

    ResultPlusGiddyUp = TestResult ++
        [{giddyup_url, list_to_binary(Base)}],
    [rt:post_result(ResultPlusGiddyUp, WebHook) ||
        WebHook <- get_webhooks()],

    %% Upload all the ct_logs as an HTML tar file
    upload_ct_logs(Base),
    add_ct_logs_to_tar(Tar),
    erl_tar:close(Tar),

    %% Finally upload the collection of artifacts as a tar file
    {ok, Contents} = file:read_file(TarFile),
    post_artifact(Base, {"artifacts.tar.gz", Contents}),
    file:delete(TarFile).

post_artifact_and_add_to_tar(Base, Tar, {Name, Contents}) ->
    post_artifact(Base, {Name, Contents}),
    ok = erl_tar:add(Tar, Contents, Name, []).

post_artifact(TRURL, {FName, Body}) ->
    %% First compute the path of where to post the artifact
    URL = artifact_url(TRURL, FName),
    ReqBody = make_req_body(Body),
    CType = guess_ctype(FName),
    %% Send request
    try ibrowse:send_req(URL, [{"Content-Type", CType}],
                         post,
                         ReqBody,
                         [{content_type, CType}, basic_auth()],
                         300000) of
        {ok, [$2|_], Headers, _Body} ->
            {_, Location} = lists:keyfind("Location", 1, Headers),
            lager:info("Successfully uploaded test artifact ~s to GiddyUp! URL: ~s", [FName, Location]),
            ok;
        {ok, RC, Headers, Body} ->
            lager:info("Test artifact ~s failed to upload!", [FName]),
            lager:debug("Status: ~p~nHeaders: ~p~nBody: ~s~n", [RC, Headers, Body]),
            error;
        X ->
            lager:error("Error uploading ~s to giddyup. ~p~n"
                        "URL: ~p~nRequest Body: ~p~nContent Type: ~p~n",
                        [FName, X, URL, ReqBody, CType]),
            error
    catch
        Throws ->
            lager:error("Error uploading ~s to giddyup. ~p~n"
                        "URL: ~p~nRequest Body: ~p~nContent Type: ~p~n",
                        [FName, Throws, URL, ReqBody, CType])
    end.


basic_auth() ->
    {basic_auth, {rt_config:get(giddyup_user), rt_config:get(giddyup_password)}}.

%% Given a URI parsed by http_uri, reconstitute it.
generate({_Scheme, _UserInfo, _Host, _Port, _Path, _Query}=URI) ->
    generate(URI, http_uri:scheme_defaults()).

generate({Scheme, UserInfo, Host, Port, Path, Query}, SchemeDefaults) ->
    {Scheme, DefaultPort} = lists:keyfind(Scheme, 1, SchemeDefaults),
    lists:flatten([
                   [ atom_to_list(Scheme), "://" ],
                   [ [UserInfo, "@"] || UserInfo /= [] ],
                   Host,
                   [ [$:, integer_to_list(Port)] || Port /= DefaultPort ],
                   Path, Query
                  ]).

%% Given the test result URL, constructs the appropriate URL for the artifact.
artifact_url(TRURL, FName) ->
    {ok, {Scheme, UserInfo, Host, Port, Path, Query}} = http_uri:parse(TRURL),
    ArtifactPath = filename:join([Path, "artifacts", FName]),
    generate({Scheme, UserInfo, Host, Port, ArtifactPath, Query}).

%% ibrowse support streaming request bodies, so in the case where we
%% have a Port/File to read from, we should stream it.
make_req_body(Body) when is_port(Body); is_pid(Body) ->
    read_fully(Body);
make_req_body(Body) when is_list(Body);
                         is_binary(Body) ->
    Body.

%% Read the file/port fully until eof. This is a workaround for the
%% fact that ibrowse doesn't seem to send file streams correctly, or
%% giddyup dislikes them. (shrug)
read_fully(File) ->
    read_fully(File, <<>>).

read_fully(File, Data0) ->
    case file:read(File, ?STREAM_CHUNK_SIZE) of
        {ok, Data} ->
            read_fully(File, <<Data0/binary, Data/binary>>);
        eof ->
            Data0
    end.

%% Guesses the content type of the file being uploaded.
guess_ctype(FName) ->
    case string:tokens(filename:basename(FName), ".") of
        [_, "log"|_] -> "text/plain"; %% console.log, erlang.log.5, etc
        ["erl_crash", "dump"] -> "text/plain"; %% An erl_crash.dump file
        [_, "html", "tar", "gz"] -> "binary/tgz-website"; %% Entire static website
        [_, Else] ->
            case mochiweb_mime:from_extension(Else) of
                undefined -> "binary/octet-stream";
                CTG -> CTG
            end;
        _ -> "binary/octet-stream"
    end.

%% Upload a tar file of just the common test logs to be a web site
upload_ct_logs(Base) ->
    TarFile = "/tmp/ct_logs" ++ integer_to_list(erlang:phash2(make_ref())),
    ok = erl_tar:create(TarFile, ["ct_logs"], [write, compressed]),
    {ok, Contents} = file:read_file(TarFile),
    giddyup:post_artifact(Base, {"ct_logs.html.tar.gz", Contents}),
    file:delete(TarFile).

%% Create a tar file of all artifacts
create_tar_file() ->
    TarFile = "/tmp/all_artifacts" ++ integer_to_list(erlang:phash2(make_ref())),
    {ok, Tar} = erl_tar:open(TarFile, [write, compressed]),
    {Tar, TarFile}.

%% Add everything in the ct_logs directory to the tar file
%% This second pass is required due to limitations in reading and writing
%% tar files in erl_tar.
add_ct_logs_to_tar(Tar) ->
    AddFileFun = fun(FileName, Acc) ->
        {ok, Contents} = file:read_file(FileName),
        ok = erl_tar:add(Tar, Contents, FileName, []),
        Acc
    end,
    filelib:fold_files("ct_logs", ".*", true, AddFileFun, []).

maybe_post_debug_logs(Base, Tar) ->
    case rt_config:get(giddyup_post_debug_logs, true) of
        true ->
            NodeDebugLogs = rt:get_node_debug_logs(),
            lists:foreach(fun({Name, Contents}) ->
                giddyup:post_artifact(Base, {Name, Contents}),
                erl_tar:add(Tar, Contents, Name, [])
                end, NodeDebugLogs);
        _ ->
            false
    end.

get_webhooks() ->
    Hooks = lists:foldl(fun(E, Acc) -> [parse_webhook(E) | Acc] end,
        [],
        rt_config:get(webhooks, [])),
    lists:filter(fun(E) -> E =/= undefined end, Hooks).

parse_webhook(Props) ->
    Url = proplists:get_value(url, Props),
    case is_list(Url) of
        true ->
            #rt_webhook{url= Url,
                name=proplists:get_value(name, Props, "Webhook"),
                headers=proplists:get_value(headers, Props, [])};
        false ->
            lager:error("Invalid configuration for webhook : ~p", Props),
            undefined
    end.
