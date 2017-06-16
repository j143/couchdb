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

-module(ddoc_cache_lru).
-behaviour(gen_server).
-vsn(1).


-export([
    start_link/0,
    open/1,
    refresh/2
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-export([
    handle_db_event/3
]).


-include("ddoc_cache.hrl").


-record(st, {
    pids, % pid -> key
    dbs, % dbname -> docid -> key -> pid
    size,
    evictor
}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


open(Key) ->
    try ets:lookup(?CACHE, Key) of
        [] ->
            lru_start(Key);
        [#entry{pid = undefined}] ->
            lru_start(Key);
        [#entry{val = undefined, pid = Pid}] ->
            couch_stats:increment_counter([ddoc_cache, miss]),
            ddoc_cache_entry:open(Pid, Key);
        [#entry{val = Val, pid = Pid}] ->
            couch_stats:increment_counter([ddoc_cache, hit]),
            ddoc_cache_entry:accessed(Pid),
            {ok, Val}
    catch _:_ ->
        couch_stats:increment_counter([ddoc_cache, recovery]),
        ddoc_cache_entry:recover(Key)
    end.


refresh(DbName, DDocIds) ->
    gen_server:cast(?MODULE, {refresh, DbName, DDocIds}).


init(_) ->
    process_flag(trap_exit, true),
    {ok, Pids} = khash:new(),
    {ok, Dbs} = khash:new(),
    {ok, Evictor} = couch_event:link_listener(
            ?MODULE, handle_db_event, nil, [all_dbs]
        ),
    {ok, #st{
        pids = Pids,
        dbs = Dbs,
        size = 0,
        evictor = Evictor
    }}.


terminate(_Reason, St) ->
    case is_pid(St#st.evictor) of
        true -> catch exit(St#st.evictor, kill);
        false -> ok
    end,
    ok.


handle_call({start, Key}, _From, St) ->
    #st{
        pids = Pids,
        dbs = Dbs,
        size = CurSize
    } = St,
    case ets:lookup(?CACHE, Key) of
        [] ->
            MaxSize = config:get_integer("ddoc_cache", "max_size", 1000),
            case trim(St, CurSize, max(0, MaxSize)) of
                {ok, N} ->
                    true = ets:insert_new(?CACHE, #entry{key = Key}),
                    {ok, Pid} = ddoc_cache_entry:start_link(Key),
                    true = ets:update_element(?CACHE, Key, {#entry.pid, Pid}),
                    ok = khash:put(Pids, Pid, Key),
                    store_key(Dbs, Key, Pid),
                    {reply, {ok, Pid}, St#st{size = CurSize - N + 1}};
                full ->
                    ?EVENT(full, Key),
                    {reply, full, St}
            end;
        [#entry{pid = Pid}] ->
            {reply, {ok, Pid}, St}
    end;

handle_call(Msg, _From, St) ->
    {stop, {invalid_call, Msg}, {invalid_call, Msg}, St}.


handle_cast({evict, DbName}, St) ->
    gen_server:abcast(mem3:nodes(), ?MODULE, {do_evict, DbName}),
    {noreply, St};

handle_cast({refresh, DbName, DDocIds}, St) ->
    gen_server:abcast(mem3:nodes(), ?MODULE, {do_refresh, DbName, DDocIds}),
    {noreply, St};

handle_cast({do_evict, DbName}, St) ->
    #st{
        dbs = Dbs
    } = St,
    ToRem = case khash:lookup(Dbs, DbName) of
        {value, DDocIds} ->
            AccOut = khash:fold(DDocIds, fun(_, Keys, Acc1) ->
                khash:to_list(Keys) ++ Acc1
            end, []),
            ?EVENT(evicted, DbName),
            AccOut;
        not_found ->
            ?EVENT(evict_noop, DbName),
            []
    end,
    lists:foreach(fun({Key, Pid}) ->
        remove_entry(St, Key, Pid)
    end, ToRem),
    khash:del(Dbs, DbName),
    {noreply, St};

handle_cast({do_refresh, DbName, DDocIdList}, St) ->
    #st{
        dbs = Dbs
    } = St,
    case khash:lookup(Dbs, DbName) of
        {value, DDocIds} ->
            lists:foreach(fun(DDocId) ->
                case khash:lookup(DDocIds, DDocId) of
                    {value, Keys} ->
                        khash:fold(Keys, fun(_, Pid, _) ->
                            ddoc_cache_entry:refresh(Pid)
                        end, nil);
                    not_found ->
                        ok
                end
            end, [no_ddocid | DDocIdList]);
        not_found ->
            ok
    end,
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {invalid_cast, Msg}, St}.


handle_info({'EXIT', Pid, _Reason}, #st{evictor = Pid} = St) ->
    ?EVENT(evictor_died, Pid),
    {ok, Evictor} = couch_event:link_listener(
            ?MODULE, handle_db_event, nil, [all_dbs]
        ),
    {noreply, St#st{evictor=Evictor}};

handle_info({'EXIT', Pid, normal}, St) ->
    % This clause handles when an entry starts
    % up but encounters an error or uncacheable
    % response from its recover call.
    #st{
        pids = Pids
    } = St,
    {value, Key} = khash:lookup(Pids, Pid),
    khash:del(Pids, Pid),
    remove_key(St, Key),
    {noreply, St};

handle_info(Msg, St) ->
    {stop, {invalid_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


handle_db_event(ShardDbName, created, St) ->
    gen_server:cast(?MODULE, {evict, mem3:dbname(ShardDbName)}),
    {ok, St};

handle_db_event(ShardDbName, deleted, St) ->
    gen_server:cast(?MODULE, {evict, mem3:dbname(ShardDbName)}),
    {ok, St};

handle_db_event(_DbName, _Event, St) ->
    {ok, St}.


lru_start(Key) ->
    case gen_server:call(?MODULE, {start, Key}, infinity) of
        {ok, Pid} ->
            couch_stats:increment_counter([ddoc_cache, miss]),
            ddoc_cache_entry:open(Pid, Key);
        full ->
            couch_stats:increment_counter([ddoc_cache, recovery]),
            ddoc_cache_entry:recover(Key)
    end.


trim(_, _, 0) ->
    full;

trim(_St, CurSize, MaxSize) when CurSize < MaxSize ->
    {ok, 0};

trim(St, CurSize, MaxSize) when CurSize >= MaxSize ->
    case ets:first(?LRU) of
        '$end_of_table' ->
            full;
        {_Ts, Key, Pid} ->
            remove_entry(St, Key, Pid),
            {ok, 1}
    end.


remove_entry(St, Key, Pid) ->
    #st{
        pids = Pids
    } = St,
    unlink(Pid),
    ddoc_cache_entry:shutdown(Pid),
    khash:del(Pids, Pid),
    remove_key(St, Key).


store_key(Dbs, Key, Pid) ->
    DbName = ddoc_cache_entry:dbname(Key),
    DDocId = ddoc_cache_entry:ddocid(Key),
    case khash:lookup(Dbs, DbName) of
        {value, DDocIds} ->
            case khash:lookup(DDocIds, DDocId) of
                {value, Keys} ->
                    khash:put(Keys, Key, Pid);
                not_found ->
                    {ok, Keys} = khash:from_list([{Key, Pid}]),
                    khash:put(DDocIds, DDocId, Keys)
            end;
        not_found ->
            {ok, Keys} = khash:from_list([{Key, Pid}]),
            {ok, DDocIds} = khash:from_list([{DDocId, Keys}]),
            khash:put(Dbs, DbName, DDocIds)
    end.


remove_key(St, Key) ->
    #st{
        dbs = Dbs
    } = St,
    DbName = ddoc_cache_entry:dbname(Key),
    DDocId = ddoc_cache_entry:ddocid(Key),
    {value, DDocIds} = khash:lookup(Dbs, DbName),
    {value, Keys} = khash:lookup(DDocIds, DDocId),
    khash:del(Keys, Key),
    case khash:size(Keys) of
        0 -> khash:del(DDocIds, DDocId);
        _ -> ok
    end,
    case khash:size(DDocIds) of
        0 -> khash:del(Dbs, DbName);
        _ -> ok
    end.
