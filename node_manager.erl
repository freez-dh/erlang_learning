-module(node_manager).
-compile(export_all).

-behavior(gen_server).
-export([start/0, init/1, handle_cast/2, handle_call/3, handle_info/2, code_change/3, terminate/2]).

-include("log.hrl").
-include("const.hrl").

% ==================gen_server callbacks=============================
start() ->
	gen_server:start({local, ?NODE_MANAGER}, ?MODULE, [], []).

init(_Args) ->
	{ok, []}.

handle_cast(_Request, State) ->
	{noreply, State}.

handle_call({register, Node}, _From, Nodes) ->
	log("Received register", ["RegisterNode", Node]),
	monitor_node(Node, true),
	{reply, {ok, Nodes}, [Node | Nodes]}.

handle_info({nodedown, DownNode}, State) ->
	log("Received nodedown", ["DownNode", DownNode]),
	NewState = lists:delete(DownNode, State),
	lists:foreach(fun(Node) ->
						  gen_server:cast({?ROUTER_NAME, Node}, {nodedown, DownNode})
				  end, NewState),
	{noreply, NewState}.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

terminate(_Reason, _State) ->
	ok.

% ==========================apis============================
%
wait_for_node_manager_connected(0) ->
	{error, connect_timeout};
wait_for_node_manager_connected(RemainTime) ->
	case lists:any(fun(Node) -> Node =:= ?NODE_MANAGER_NODE end, nodes()) of
		true ->
			ok;
		false ->
			log("Wait 1 sec for node connected"),
			wait_for_node_manager_connected(RemainTime - 1)
	end.

contact_node_manager() ->
	case net_adm:ping(?NODE_MANAGER_NODE) of
		pong ->
			wait_for_node_manager_connected(10);
		pang ->
			{error, ping_failed}
	end.

register_node() ->
	case contact_node_manager() of
		ok ->
			gen_server:call({?NODE_MANAGER, ?NODE_MANAGER_NODE}, {register, node()});
		{error, Reason} ->
			log("Contact node manager failed", ["Reason", Reason]),
			{error, Reason}
	end.

