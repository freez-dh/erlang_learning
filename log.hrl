-compile({nowarn_unused_function, [current_time/0, log_format/1, log/1, log/2]}).
current_time() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
    io_lib:format("[~B-~B-~B ~B:~B:~B]", [Year, Month, Day, Hour, Min, Sec]).

log_format([]) ->
    "";

log_format([_Key, _Value | Tail]) ->
    "~s=~p," ++ log_format(Tail).

log(Msg) ->
    log(Msg, []).

log(Msg, KeyValues) ->
    KeyValueFormat = log_format(KeyValues),
    Format = "~s[~p]~s," ++ KeyValueFormat ++ "\n",
    case KeyValues of
        [] ->
            io:format(Format, [current_time(), self(), Msg]);
        _ ->
            io:format(Format, [current_time(), self(), Msg | KeyValues])
    end.

