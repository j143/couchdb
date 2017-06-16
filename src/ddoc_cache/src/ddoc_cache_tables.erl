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

-module(ddoc_cache_tables).
-behaviour(gen_server).
-vsn(1).


-export([
    start_link/0
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-include("ddoc_cache.hrl").


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    BaseOpts = [public, named_table],
    CacheOpts = [
        set,
        {read_concurrency, true},
        {keypos, #entry.key}
    ] ++ BaseOpts,
    ets:new(?CACHE, CacheOpts),
    ets:new(?LRU, [ordered_set, {write_concurrency, true}] ++ BaseOpts),
    {ok, nil}.


terminate(_Reason, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
