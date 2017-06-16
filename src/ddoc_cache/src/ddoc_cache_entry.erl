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

-module(ddoc_cache_entry).


-export([
    dbname/1,
    ddocid/1,
    recover/1,

    start_link/1,
    shutdown/1,
    open/2,
    accessed/1,
    refresh/1
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
    do_open/1
]).


-include("ddoc_cache.hrl").


-record(st, {
    key,
    val,
    opener,
    waiters,
    ts
}).


dbname({Mod, Arg}) ->
    Mod:dbname(Arg).


ddocid({Mod, Arg}) ->
    Mod:ddocid(Arg).


recover({Mod, Arg}) ->
    Mod:recover(Arg).


start_link(Key) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Key]),
    {ok, Pid}.


shutdown(Pid) ->
    ok = gen_server:call(Pid, shutdown).


open(Pid, Key) ->
    try
        Resp = gen_server:call(Pid, open),
        case Resp of
            {open_ok, Val} ->
                Val;
            {open_error, {T, R, S}} ->
                erlang:raise(T, R, S)
        end
    catch exit:_ ->
        % Its possible that this process was evicted just
        % before we tried talking to it. Just fallback
        % to a standard recovery
        recover(Key)
    end.


accessed(Pid) ->
    gen_server:cast(Pid, accessed).


refresh(Pid) ->
    gen_server:cast(Pid, refresh).


init(Key) ->
    true = ets:update_element(?CACHE, Key, {#entry.pid, self()}),
    St = #st{
        key = Key,
        opener = spawn_opener(Key),
        waiters = []
    },
    ?EVENT(started, Key),
    gen_server:enter_loop(?MODULE, [], St).


terminate(_Reason, St) ->
    #st{
        key = Key,
        opener = Pid,
        ts = Ts
    } = St,
    % We may have already deleted our cache entry
    % during shutdown
    Pattern = #entry{key = Key, pid = self(), _ = '_'},
    CacheMSpec = [{Pattern, [], [true]}],
    true = ets:select_delete(?CACHE, CacheMSpec) < 2,
    % We may have already deleted our LRU entry
    % during shutdown
    if Ts == undefined -> ok; true ->
        LruMSpec = [{{{Ts, Key, self()}}, [], [true]}],
        true = ets:select_delete(?LRU, LruMSpec) < 2
    end,
    % Blow away any current opener if it exists
    if not is_pid(Pid) -> ok; true ->
        catch exit(Pid, kill)
    end,
    ok.


handle_call(open, From, #st{val = undefined} = St) ->
    NewSt = St#st{
        waiters = [From | St#st.waiters]
    },
    {noreply, NewSt};

handle_call(open, _From, St) ->
    {reply, St#st.val, St};

handle_call(shutdown, _From, St) ->
    remove_from_cache(St),
    {stop, normal, ok, St};

handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(accessed, St) ->
    ?EVENT(accessed, St#st.key),
    drain_accessed(),
    {noreply, update_lru(St)};

handle_cast(refresh, #st{opener = Ref} = St) when is_reference(Ref) ->
    #st{
        key = Key
    } = St,
    erlang:cancel_timer(Ref),
    NewSt = St#st{
        opener = spawn_opener(Key)
    },
    {noreply, NewSt};

handle_cast(refresh, #st{opener = Pid} = St) when is_pid(Pid) ->
    catch exit(Pid, kill),
    receive
        {'DOWN', _, _, Pid, _} -> ok
    end,
    NewSt = St#st{
        opener = spawn_opener(St#st.key)
    },
    {noreply, NewSt};

handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info({'DOWN', _, _, Pid, Resp}, #st{key = Key, opener = Pid} = St) ->
    case Resp of
        {open_ok, Key, {ok, Val}} ->
            if not is_list(St#st.waiters) -> ok; true ->
                respond(St#st.waiters, {open_ok, {ok, Val}})
            end,
            update_cache(St, Val),
            Msg = {'$gen_cast', refresh},
            Timer = erlang:send_after(?REFRESH_TIMEOUT, self(), Msg),
            NewSt = St#st{
                val = {open_ok, {ok, Val}},
                opener = Timer
            },
            {noreply, update_lru(NewSt)};
        {Status, Key, Other} ->
            NewSt = St#st{
                val = {Status, Other},
                opener = undefined,
                waiters = undefined
            },
            remove_from_cache(NewSt),
            if not is_list(St#st.waiters) -> ok; true ->
                respond(St#st.waiters, {Status, Other})
            end,
            {stop, normal, NewSt}
    end;

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_, St, _) ->
    {ok, St}.


spawn_opener(Key) ->
    {Pid, _} = erlang:spawn_monitor(?MODULE, do_open, [Key]),
    Pid.


do_open(Key) ->
    try recover(Key) of
        Resp ->
            erlang:exit({open_ok, Key, Resp})
    catch T:R ->
        S = erlang:get_stacktrace(),
        erlang:exit({open_error, Key, {T, R, S}})
    end.


update_lru(#st{key = Key, ts = Ts} = St) ->
    if Ts == undefined -> ok; true ->
        MSpec = [{{{Ts, Key, self()}}, [], [true]}],
        1 = ets:select_delete(?LRU, MSpec)
    end,
    NewTs = os:timestamp(),
    true = ets:insert(?LRU, {{NewTs, Key, self()}}),
    St#st{ts = NewTs}.


update_cache(#st{val = undefined} = St, Val) ->
    true = ets:update_element(?CACHE, St#st.key, {#entry.val, Val}),
    ?EVENT(inserted, St#st.key);

update_cache(#st{val = V1} = _St, V2) when {open_ok, {ok, V2}} == V1 ->
    ?EVENT(update_noop, _St#st.key);

update_cache(St, Val) ->
    true = ets:update_element(?CACHE, St#st.key, {#entry.val, Val}),
    ?EVENT(updated, {St#st.key, Val}).


remove_from_cache(St) ->
    #st{
        key = Key,
        ts = Ts
    } = St,
    Pattern = #entry{key = Key, pid = self(), _ = '_'},
    CacheMSpec = [{Pattern, [], [true]}],
    1 = ets:select_delete(?CACHE, CacheMSpec),
    if Ts == undefined -> ok; true ->
        LruMSpec = [{{{Ts, Key, self()}}, [], [true]}],
        1 = ets:select_delete(?LRU, LruMSpec)
    end,
    ?EVENT(removed, St#st.key),
    ok.


drain_accessed() ->
    receive
        {'$gen_cast', accessed} ->
            drain_accessed()
    after 0 ->
        ok
    end.


respond(Waiters, Resp) ->
    [gen_server:reply(W, Resp) || W <- Waiters].
