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

-module(ddoc_cache_basic_test).


-export([
    recover/1
]).


-include_lib("couch/include/couch_db.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("ddoc_cache_test.hrl").


recover(DbName) ->
    {ok, {DbName, totes_custom}}.


check_basic_test_() ->
    {
        setup,
        fun ddoc_cache_tutil:start_couch/0,
        fun ddoc_cache_tutil:stop_couch/1,
        {with, [
            fun cache_ddoc/1,
            fun cache_ddoc_rev/1,
            fun cache_vdu/1,
            fun cache_custom/1,
            fun cache_ddoc_refresher_unchanged/1,
            fun dont_cache_not_found/1,
            fun deprecated_api_works/1
        ]}
    }.


cache_ddoc({DbName, _}) ->
    ddoc_cache_tutil:clear(),
    ?assertEqual(0, ets:info(?CACHE, size)),
    Resp1 = ddoc_cache:open_doc(DbName, ?FOOBAR),
    ?assertMatch({ok, #doc{id = ?FOOBAR}}, Resp1),
    ?assertEqual(1, ets:info(?CACHE, size)),
    Resp2 = ddoc_cache:open_doc(DbName, ?FOOBAR),
    ?assertEqual(Resp1, Resp2),
    ?assertEqual(1, ets:info(?CACHE, size)).


cache_ddoc_rev({DbName, _}) ->
    ddoc_cache_tutil:clear(),
    Rev = ddoc_cache_tutil:get_rev(DbName, ?FOOBAR),
    ?assertEqual(0, ets:info(?CACHE, size)),
    Resp1 = ddoc_cache:open_doc(DbName, ?FOOBAR, Rev),
    ?assertMatch({ok, #doc{id = ?FOOBAR}}, Resp1),
    ?assertEqual(1, ets:info(?CACHE, size)),
    Resp2 = ddoc_cache:open_doc(DbName, ?FOOBAR, Rev),
    ?assertEqual(Resp1, Resp2),
    ?assertEqual(1, ets:info(?CACHE, size)),

    % Assert that the non-rev cache entry is separate
    Resp3 = ddoc_cache:open_doc(DbName, ?FOOBAR),
    ?assertMatch({ok, #doc{id = ?FOOBAR}}, Resp3),
    ?assertEqual(2, ets:info(?CACHE, size)).


cache_vdu({DbName, _}) ->
    ddoc_cache_tutil:clear(),
    ?assertEqual(0, ets:info(?CACHE, size)),
    Resp1 = ddoc_cache:open_validation_funs(DbName),
    ?assertMatch({ok, [_]}, Resp1),
    ?assertEqual(1, ets:info(?CACHE, size)),
    Resp2 = ddoc_cache:open_validation_funs(DbName),
    ?assertEqual(Resp1, Resp2),
    ?assertEqual(1, ets:info(?CACHE, size)).


cache_custom({DbName, _}) ->
    ddoc_cache_tutil:clear(),
    ?assertEqual(0, ets:info(?CACHE, size)),
    Resp1 = ddoc_cache:open_custom(DbName, ?MODULE),
    ?assertMatch({ok, {DbName, totes_custom}}, Resp1),
    ?assertEqual(1, ets:info(?CACHE, size)),
    Resp2 = ddoc_cache:open_custom(DbName, ?MODULE),
    ?assertEqual(Resp1, Resp2),
    ?assertEqual(1, ets:info(?CACHE, size)).


cache_ddoc_refresher_unchanged({DbName, _}) ->
    ddoc_cache_tutil:clear(),
    ?assertEqual(0, ets:info(?CACHE, size)),
    ddoc_cache:open_doc(DbName, ?FOOBAR),
    [Entry1] = ets:lookup(?CACHE, ets:first(?CACHE)),
    ddoc_cache:open_doc(DbName, ?FOOBAR),
    [Entry2] = ets:lookup(?CACHE, ets:first(?CACHE)),
    ?assertEqual(Entry1, Entry2).


dont_cache_not_found({DbName, _}) ->
    DDocId = <<"_design/not_found">>,
    ddoc_cache_tutil:clear(),
    Resp = ddoc_cache:open_doc(DbName, DDocId),
    ?assertEqual({not_found, missing}, Resp),
    ?assertEqual(0, ets:info(?CACHE, size)),
    ?assertEqual(0, ets:info(?LRU, size)).


deprecated_api_works({DbName, _}) ->
    ddoc_cache_tutil:clear(),
    {ok, _} = ddoc_cache:open(DbName, ?FOOBAR),
    {ok, _} = ddoc_cache:open(DbName, <<"foobar">>),
    {ok, _} = ddoc_cache:open(DbName, ?MODULE),
    {ok, _} = ddoc_cache:open(DbName, validation_funs).

