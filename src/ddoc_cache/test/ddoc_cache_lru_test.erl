% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(ddoc_cache_lru_test).


-export([
    recover/1
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("ddoc_cache_test.hrl").


recover(<<"pause", _/binary>>) ->
    receive go -> ok end,
    {ok, paused};

recover(DbName) ->
    {ok, DbName}.


start_couch() ->
    Ctx = ddoc_cache_tutil:start_couch(),
    config:set("ddoc_cache", "max_size", "5", false),
    meck:new(ddoc_cache_ev, [passthrough]),
    Ctx.


stop_couch(Ctx) ->
    meck:unload(),
    ddoc_cache_tutil:stop_couch(Ctx).


check_not_started_test() ->
    % Starting couch, but not ddoc_cache
    Ctx = test_util:start_couch(),
    try
        Key = {ddoc_cache_entry_custom, {<<"dbname">>, ?MODULE}},
        ?assertEqual({ok, <<"dbname">>}, ddoc_cache_lru:open(Key))
    after
        test_util:stop_couch(Ctx)
    end.


check_lru_test_() ->
    {
        setup,
        fun start_couch/0,
        fun stop_couch/1,
        {with, [
            fun check_multi_start/1,
            fun check_multi_open/1,
            fun check_capped_size/1,
            fun check_full_cache/1
        ]}
    }.


check_multi_start(_) ->
    ddoc_cache_tutil:clear(),
    meck:reset(ddoc_cache_ev),
    Key = {ddoc_cache_entry_custom, {<<"pause">>, ?MODULE}},
    % These will all get sent through ddoc_cache_lru
    Clients = lists:map(fun(_) ->
        spawn_monitor(fun() ->
            ddoc_cache_lru:open(Key)
        end)
    end, lists:seq(1, 10)),
    meck:wait(ddoc_cache_ev, event, [started, Key], 1000),
    lists:foreach(fun({Pid, _Ref}) ->
        ?assert(is_process_alive(Pid))
    end, Clients),
    [#entry{pid = Pid}] = ets:tab2list(?CACHE),
    Opener = element(4, sys:get_state(Pid)),
    OpenerRef = erlang:monitor(process, Opener),
    ?assert(is_process_alive(Opener)),
    Opener ! go,
    receive {'DOWN', OpenerRef, _, _, _} -> ok end,
    lists:foreach(fun({CPid, Ref}) ->
        receive
            {'DOWN', Ref, _, _, normal} -> ok;
            {'DOWN', Ref, _, _, Other} ->
                io:format(standard_error, "OTHER: ~p~n", [Other]);
            Other when not is_tuple(Other) orelse element(1, Other) /= 'DOWN' ->
                io:format(standard_error, "MSG: ~p~n", [Other])
        after 2000 ->
            io:format(standard_error, "BLAH?!: ~p ~p", [CPid, process_info(CPid, current_stacktrace)])
        end
    end, Clients).


check_multi_open(_) ->
    ddoc_cache_tutil:clear(),
    meck:reset(ddoc_cache_ev),
    Key = {ddoc_cache_entry_custom, {<<"pause">>, ?MODULE}},
    % We wait after the first client so that
    % the rest of the clients go directly to
    % ddoc_cache_entry bypassing ddoc_cache_lru
    Client1 = spawn_monitor(fun() ->
        ddoc_cache_lru:open(Key)
    end),
    meck:wait(ddoc_cache_ev, event, [started, Key], 1000),
    Clients = [Client1] ++ lists:map(fun(_) ->
        spawn_monitor(fun() ->
            ddoc_cache_lru:open(Key)
        end)
    end, lists:seq(1, 9)),
    lists:foreach(fun({Pid, _Ref}) ->
        ?assert(is_process_alive(Pid))
    end, Clients),
    [#entry{pid = Pid}] = ets:tab2list(?CACHE),
    Opener = element(4, sys:get_state(Pid)),
    OpenerRef = erlang:monitor(process, Opener),
    ?assert(is_process_alive(Opener)),
    Opener ! go,
    receive {'DOWN', OpenerRef, _, _, _} -> ok end,
    lists:foreach(fun({_, Ref}) ->
        receive {'DOWN', Ref, _, _, normal} -> ok end
    end, Clients).


check_capped_size(_) ->
    ddoc_cache_tutil:clear(),
    meck:reset(ddoc_cache_ev),
    lists:foreach(fun(I) ->
        DbName = list_to_binary(integer_to_list(I)),
        ddoc_cache:open_custom(DbName, ?MODULE),
        meck:wait(I, ddoc_cache_ev, event, [started, '_'], 1000),
        ?assertEqual(I, ets:info(?CACHE, size))
    end, lists:seq(1, 5)),
    lists:foreach(fun(I) ->
        DbName = list_to_binary(integer_to_list(I)),
        ddoc_cache:open_custom(DbName, ?MODULE),
        meck:wait(I, ddoc_cache_ev, event, [started, '_'], 1000),
        ?assertEqual(5, ets:info(?CACHE, size))
    end, lists:seq(6, 20)).


check_full_cache(_) ->
    ddoc_cache_tutil:clear(),
    meck:reset(ddoc_cache_ev),
    lists:foreach(fun(I) ->
        DbSuffix = list_to_binary(integer_to_list(I)),
        DbName = <<"pause", DbSuffix/binary>>,
        spawn(fun() -> ddoc_cache:open_custom(DbName, ?MODULE) end),
        meck:wait(I, ddoc_cache_ev, event, [started, '_'], 1000),
        ?assertEqual(I, ets:info(?CACHE, size))
    end, lists:seq(1, 5)),
    lists:foreach(fun(I) ->
        DbSuffix = list_to_binary(integer_to_list(I)),
        DbName = <<"pause", DbSuffix/binary>>,
        spawn(fun() -> ddoc_cache:open_custom(DbName, ?MODULE) end),
        meck:wait(I - 5, ddoc_cache_ev, event, [full, '_'], 1000),
        ?assertEqual(5, ets:info(?CACHE, size))
    end, lists:seq(6, 20)).
