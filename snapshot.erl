-module(snapshot).
-compile(export_all).
-include("log.hrl").

snapshot(RouterPid, [], _PidSize) ->
    log("Pids length is zero,skip snapshot"),
    timer:sleep(2000),
    RouterPid ! start_snapshot;

snapshot(RouterPid, Pids, PidSize) ->
    [StarterPid | _] = Pids,
    StarterPid ! {start_snapshot, self(), Pids},
    snapshot_wait(RouterPid, dict:new(), dict:new(), Pids, PidSize).

is_local_states_complete(_LocalStates, []) ->
    true;
is_local_states_complete(LocalStates, [Pid | Remain]) ->
    case dict:find(Pid, LocalStates) of
        {ok, _} ->
            is_local_states_complete(LocalStates, Remain);
        error ->
            false
    end.

is_channel_states_complete(_AllChannelStates, [], _PidSize) ->
    true;
is_channel_states_complete(AllChannelStates, [Pid | Remain], PidSize) ->
    Res = dict:is_key(Pid, AllChannelStates),
    Res and is_channel_states_complete(AllChannelStates, Remain, PidSize).

is_snapshot_done(LocalStates, AllChannelStates, Pids, PidSize) ->
    StateComplete = is_local_states_complete(LocalStates, Pids),
    ChannelComplete = is_channel_states_complete(AllChannelStates, Pids, PidSize),
    StateComplete and ChannelComplete.

snapshot_wait_handle_packet(RouterPid, LocalStates, AllChannelStates, Pids, PidSize) ->
    receive
        {local_state, Pid, CurrentMoney} ->
            log("Received local state", ["Pid", Pid, "CurrentMoney", CurrentMoney]),
            FindResult = dict:find(Pid, LocalStates),
            case FindResult of
                {ok, Result} ->
                    log("!!!!Duplicate local state!!!!",
                        ["Pid", Pid, "SavedState", Result, "ReceivedState", CurrentMoney]);
                error ->
                    NewLocalStates = dict:store(Pid, CurrentMoney, LocalStates),
                    snapshot_wait(RouterPid, NewLocalStates, AllChannelStates, Pids, PidSize)
            end;
        {channel_state, Pid, ChannelStates} ->
            log("Received channel state", ["Pid", Pid, "ChannelStates", ChannelStates]),
            NewAllChannelStates = dict:store(Pid, ChannelStates, AllChannelStates),
            snapshot_wait(RouterPid, LocalStates, NewAllChannelStates, Pids, PidSize)
    end.

snapshot_wait(RouterPid, LocalStates, AllChannelStates, Pids, PidSize) ->
    case is_snapshot_done(LocalStates, AllChannelStates, Pids, PidSize) of
        true ->
            log("Snapshot done", ["Pid", self()]),
            RouterPid ! start_snapshot;
        false ->
            snapshot_wait_handle_packet(RouterPid, LocalStates, AllChannelStates, Pids, PidSize)
    end.

router(Pids, Size) ->
    receive
        {new_p, Pid} ->
            % Msg = io_lib:format("New worker process ~p", [Pid]),
            log("New worker process", ["Pid", Pid]),
            router([Pid | Pids], Size + 1);
        {route_message, FromPid, OpName, OpValue} ->
            % log("Route message", ["type", OpName, "value", OpValue]),
            Choosed = random:uniform(Size),
            Pid = lists:nth(Choosed, Pids),
            Pid ! {op, FromPid, OpName, OpValue};
       start_snapshot ->
           SnapshotPid = spawn(fun() -> snapshot(self(), Pids, Size) end),
           log("Start snapshot", ["Pid", SnapshotPid])
    end,
    router(Pids, Size).

send_snapshot_tag(_SnapshotPid, [], _Pids) ->
    ok;
send_snapshot_tag(SnapshotPid, [Pid | Remain], Pids) ->
    Pid ! {snapshot_tag, SnapshotPid, self(), Pids},
    send_snapshot_tag(SnapshotPid, Remain, Pids).

handle_receive_new_tag(SnapshotPid, SnapshotTmpStates, Pids, CurMoney, NeedCreateChannelStates) ->
    SnapshotPid ! {local_state, self(), CurMoney},
    NewSnapshotTmpStates = case NeedCreateChannelStates of
        true ->
            ChannelStates = dict:new(),
            dict:store(SnapshotPid, ChannelStates, SnapshotTmpStates);
        false ->
            SnapshotTmpStates
    end,
    send_snapshot_tag(SnapshotPid, Pids, Pids),
    NewSnapshotTmpStates.

is_channel_state_complete(_ChannelStates, []) ->
    true;
is_channel_state_complete(ChannelStates, [Pid | Remain]) ->
    case dict:find(Pid, ChannelStates) of
        % 只有有结尾标签的才是通道记录完
        {ok, [snapshot_done | _]} -> is_channel_state_complete(ChannelStates, Remain);
        % 没有结束标签或者list都不存在，均是没有记录完
        _ -> false
    end.

update_snapshot_channel_state(FromPid, OpType, OpValue, SnapshotTmpStates) ->
    MapFunc = fun(_SnapshotPid, ChannelStates) ->
            NewChannelState = case dict:find(FromPid, ChannelStates) of
                {ok, [snapshot_done | _Remain]} ->
                    [snapshot_done | _Remain];
                {ok, ChannelState} -> 
                    [{FromPid, OpType, OpValue} | ChannelState];
                error ->
                    [{FromPid, OpType, OpValue}]
                end,
            dict:store(FromPid, NewChannelState, ChannelStates)
    end,
    dict:map(MapFunc, SnapshotTmpStates).

worker(CurMoney, RouterPid, SnapshotTmpStates) ->
    receive
        {start_snapshot, SnapshotPid, Pids} ->
            NewSnapshotTmpStates = handle_receive_new_tag(SnapshotPid, SnapshotTmpStates, Pids, CurMoney, true),
            worker(CurMoney, RouterPid, NewSnapshotTmpStates);
        {snapshot_tag, SnapshotPid, FromPid, Pids} ->
            log("Received snapshot_tag", ["FromPid", FromPid, "Pids", Pids]),
            case dict:find(SnapshotPid, SnapshotTmpStates) of
                % 已经收到过TAG,只需要看下CHANNEL是否搜集完成
                {ok, ChannelStates} ->
                    NewChannelState = case dict:find(FromPid, ChannelStates) of
                        {ok, ChannelState} ->
                            [snapshot_done | ChannelState];
                        error ->
                            [snapshot_done]
                    end,
                    NewChannelStates = dict:store(FromPid, NewChannelState, ChannelStates),
                    case is_channel_state_complete(NewChannelStates, Pids) of
                            % 所有的CHANNEL收集完成，发送给SNAPSHOT进程，本地的该次snapshot完成
                            true ->
                                % log("Channel state complete", ["FromPid", FromPid]),
                                SnapshotPid ! {channel_state, self(), NewChannelStates},
                                NewSnapshotTmpStates = dict:erase(SnapshotPid, SnapshotTmpStates),
                                worker(CurMoney, RouterPid, NewSnapshotTmpStates);
                            % 仍然需要等待其他的CHANNEL完成
                            false ->
                                % log("Channel state not complete", ["FromPid", FromPid, "ChannelStates", NewChannelStates]),
                                NewSnapshotTmpStates = dict:store(SnapshotPid, NewChannelStates, SnapshotTmpStates),
                                worker(CurMoney, RouterPid, NewSnapshotTmpStates)
                    end;
                error ->
                    % 没有收到过TAG，发送本地状态给SNAPSHOT进程，并且发送TAG给所有的CHANNEL
                    ChannelStates = dict:store(FromPid, [snapshot_done], dict:new()),
                    FillEmptySnapshotTmpStates = dict:store(SnapshotPid, ChannelStates, SnapshotTmpStates),
                    % log("First receive snapshot_tag", ["FromPid", FromPid, "ChannelStates", FillEmptySnapshotTmpStates]),
                    NewSnapshotTmpStates = handle_receive_new_tag(SnapshotPid, FillEmptySnapshotTmpStates, Pids, CurMoney, false),
                    % BUG?如果是叶节点，这里也应该判断是否终止的
                    worker(CurMoney, RouterPid, NewSnapshotTmpStates)
            end;
        {op, FromPid, "transfer", OpValue} ->
            % log("Received transfer", ["FromPid", FromPid, "TransferValue", OpValue, "AfterValue", CurMoney + OpValue]),
            NewSnapshotTmpStates = update_snapshot_channel_state(FromPid, "transfer", OpValue, SnapshotTmpStates),
            worker(CurMoney + OpValue, RouterPid, NewSnapshotTmpStates)
    after 0 ->
        SleepTime = random:uniform(10000),
        % log("Sleeping", ["msecs", SleepTime]),
        timer:sleep(SleepTime),
        OpTypes = {"noop", "transfer"},
        OpTypeIndex = random:uniform(tuple_size(OpTypes)),
        OpType = element(OpTypeIndex, OpTypes),
        case OpType of
            "noop" ->
                % log("Noop"),
                worker(CurMoney, RouterPid, SnapshotTmpStates);
            "transfer" ->
                case CurMoney of
                    0 -> worker(CurMoney, RouterPid, SnapshotTmpStates);
                    _ -> 
                        TransferedMoney = random:uniform(min(CurMoney, 50)),
                        RouterPid ! {route_message, self(), OpType, TransferedMoney},
                        % log("Transfer Money", ["TransferValue", TransferedMoney, "AfterValue", CurMoney - TransferedMoney]),
                        worker(CurMoney - TransferedMoney, RouterPid, SnapshotTmpStates)
                end
        end
    end.

start() ->
    io:format("Snapshot start~n", []),
    RouterPid = spawn(fun() -> router([], 0) end),
    Seqs = lists:seq(1, 3),
    lists:foreach(fun(_) -> 
                          log("Start new process"),
                          WorkerPid = spawn(fun() -> 
                                                    random:seed(now()),
                                                    worker(10000, RouterPid, dict:new()) end),
                          RouterPid ! {new_p, WorkerPid}
                    end,
                Seqs),
    timer:sleep(1500),
    RouterPid ! start_snapshot.
