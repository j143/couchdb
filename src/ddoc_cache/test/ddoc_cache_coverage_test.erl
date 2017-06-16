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

-module(ddoc_cache_coverage_test).


-include_lib("couch/include/couch_db.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("ddoc_cache_test.hrl").


coverage_test_() ->
    {
        setup,
        fun ddoc_cache_tutil:start_couch/0,
        fun ddoc_cache_tutil:stop_couch/1,
        [
            fun restart_lru/0,
            fun restart_tables/0,
            fun restart_evictor/0
        ]
    }.


restart_lru() ->
    send_bad_messages(ddoc_cache_lru),
    ?assertEqual(ok, ddoc_cache_lru:terminate(bang, {st, a, b, c, d})),
    ?assertEqual({ok, foo}, ddoc_cache_lru:code_change(1, foo, [])).


restart_tables() ->
    send_bad_messages(ddoc_cache_tables),
    ?assertEqual(ok, ddoc_cache_tables:terminate(bang, baz)),
    ?assertEqual({ok, foo}, ddoc_cache_tables:code_change(1, foo, [])).


restart_evictor() ->
    meck:new(ddoc_cache_ev, [passthrough]),
    try
        State = sys:get_state(ddoc_cache_lru),
        Evictor = element(5, State),
        Ref = erlang:monitor(process, Evictor),
        exit(Evictor, shutdown),
        receive
            {'DOWN', Ref, _, _, Reason} ->
                couch_log:error("MONITOR: ~p", [Reason]),
                ok
        end,
        meck:wait(ddoc_cache_ev, event, [evictor_died, '_'], 1000),
        NewState = sys:get_state(ddoc_cache_lru),
        NewEvictor = element(5, NewState),
        ?assertNotEqual(Evictor, NewEvictor)
    after
        meck:unload()
    end.


send_bad_messages(Name) ->
    wait_for_restart(Name, fun() ->
        ?assertEqual({invalid_call, foo}, gen_server:call(Name, foo))
    end),
    wait_for_restart(Name, fun() ->
        gen_server:cast(Name, foo)
    end),
    wait_for_restart(Name, fun() ->
        whereis(Name) ! foo
    end).


wait_for_restart(Server, Fun) ->
    Ref = erlang:monitor(process, whereis(Server)),
    Fun(),
    receive
        {'DOWN', Ref, _, _, _} ->
            ok
    end,
    ?assert(is_pid(test_util:wait_process(Server))).
