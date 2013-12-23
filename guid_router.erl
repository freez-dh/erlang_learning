-module(guid_router).

-compile(export_all).

-record(router_state, {local_guid_set, remote_guid_node}).

-behavior(gen_server).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, code_change/3, terminate/2]).

-include("log.hrl").
-include("const.hrl").

start() ->
	gen_server:start({local, ?ROUTER_NAME}, ?MODULE, [], []).

% ==============================gen_server callbacks=====================
init(_Args) ->
	random:seed(now()),
	case node_manager:register_node() of
		{ok, Nodes} ->
			log("Register node old nodes", ["Nodes", Nodes]),
			lists:foreach(fun(Node) ->
								  log("Try cast request all guid", ["Node", Node]),
								   gen_server:cast({?ROUTER_NAME, Node}, {request_all_guid, node()})
						  end, Nodes),
			{ok, #router_state{local_guid_set = dict:new(), remote_guid_node = dict:new()}};
		_  ->
			error
	end.

notify_other_nodes_router(Request) ->
	lists:foreach(
		fun(Node) ->
				gen_server:cast({?ROUTER_NAME, Node}, Request)
		end,
	nodes()).

handle_cast({add_guid, Guid, Pid}, State) ->
	LocalGuidSet = State#router_state.local_guid_set,
	NewLocalGuidSet = dict:store(Guid, Pid, LocalGuidSet),
	notify_other_nodes_router({notify_add_guid, Guid, node()}),
	{noreply, State#router_state{local_guid_set = NewLocalGuidSet}};

handle_cast({notify_add_guid, Guid, Node}, State) ->
	RemoteGuidNode = State#router_state.remote_guid_node,
	NewRemoteGuidNode = dict:store(Guid, Node, RemoteGuidNode),
	{noreply, State#router_state{remote_guid_node = NewRemoteGuidNode}};

handle_cast({remove_guid, Guid}, State) ->
	LocalGuidSet = State#router_state.local_guid_set,
	NewLocalGuidSet = dict:erase(Guid, LocalGuidSet),
	notify_other_nodes_router({notify_remove_guid, Guid, node()}),
	{noreply, State#router_state{local_guid_set = NewLocalGuidSet}};

handle_cast({notify_remove_guid, Guid, _Node}, State) ->
	RemoteGuidNode = State#router_state.remote_guid_node,
	NewRemoteGuidNode = dict:erase(Guid, RemoteGuidNode),
	{noreply, State#router_state{remote_guid_node = NewRemoteGuidNode}};

handle_cast({remote_find_guid, Guid, FromNode, Ctx}, State) ->
	LocalGuidSet = State#router_state.local_guid_set,
	Result = dict:find(Guid, LocalGuidSet),
	gen_server:cast({?ROUTER_NAME, FromNode}, {remote_reply_guid, Result, Ctx}),
	{noreply, State};

handle_cast({remote_reply_guid, ReplyInfo, CallFrom}, State) ->
	gen_server:reply(CallFrom, ReplyInfo),
	{noreply, State};

handle_cast({request_all_guid, FromNode}, State) ->
	log("in request all guid", ["FromNode", FromNode]),
	LocalGuidSet = State#router_state.local_guid_set,
	GuidList = dict:to_list(LocalGuidSet),
	Guids = [Guid || {Guid, _Pid} <- GuidList],
	gen_server:cast({?ROUTER_NAME, FromNode}, {reply_all_guid, Guids, node()}),
	{noreply, State};

handle_cast({reply_all_guid, Guids, Node}, State) ->
	log("in reply all guid", ["FromNode", Node, "Guids", Guids]),
	RemoteGuidNode = State#router_state.remote_guid_node,
	NewRemoteGuidNode = insert_all_guid(Guids, Node, RemoteGuidNode),
	{noreply, State#router_state{remote_guid_node = NewRemoteGuidNode}};

handle_cast({nodedown, DownNode}, State) ->
	log("Received nodedown", ["DownNode", DownNode]),
	{noreply, State}.

try_find_guid_remote(Guid, From, State) ->
	RemoteGuidNode = State#router_state.remote_guid_node,
	case dict:find(Guid, RemoteGuidNode) of
		{ok, Node} ->
			gen_server:cast({?ROUTER_NAME, Node}, {remote_find_guid, Guid, node(), From}),
			{noreply, State};
		error ->
			{reply, error, State}
	end.

insert_all_guid([], _Node, RemoteGuidNode) ->
	RemoteGuidNode;
insert_all_guid([Guid | Remain], Node, RemoteGuidNode) ->
	NewRemoteGuidNode = dict:store(Guid, Node, RemoteGuidNode),
	insert_all_guid(Remain, Node, NewRemoteGuidNode).

random_from_dict(Dict, DefaultGuid) ->
	case dict:size(Dict) of
		0 ->
			DefaultGuid;
		_ ->
			Guids = dict:fetch_keys(Dict),
			Index = random:uniform(length(Guids)),
			lists:nth(Index, Guids)
	end.

handle_call({find_guid, Guid}, From, State) ->
	LocalGuidSet = State#router_state.local_guid_set,
	case dict:find(Guid, LocalGuidSet) of
		{ok, Pid} ->
			{reply, {ok, Pid}, State};
		error ->
			try_find_guid_remote(Guid, From, State)
	end;

handle_call({random_guid, DefaultGuid}, _From, State) ->
	Guid = case random:uniform(2) of
		1 ->
			LocalGuidSet = State#router_state.local_guid_set,
			random_from_dict(LocalGuidSet, DefaultGuid);
		2 ->
			RemoteGuidNode = State#router_state.remote_guid_node,
			random_from_dict(RemoteGuidNode, DefaultGuid)
	end,
	{reply, Guid, State}.

handle_info(_Info, State) ->
	{noreply, State}.

code_change(_OldVersion, State, _Extra) ->
	{ok, State}.

terminate(_Reason, _State) ->
	ok.


% ================================export apis===========================
add_guid(Guid) ->
	gen_server:cast(?ROUTER_NAME, {add_guid, Guid, self()}).

remove_guid(Guid) ->
	gen_server:cast(?ROUTER_NAME, {remove_guid, Guid}).

find_guid(Guid) ->
	gen_server:call(?ROUTER_NAME, {find_guid, Guid}).

%random_guid(DefaultGuid) ->
	%gen_server:call(?ROUTER_NAME, {random_guid, DefaultGuid}).

% ==============================test================================
%

-define(GUID_COUNT, 10000).

random_guid(Index) ->
	I = random:uniform(?GUID_COUNT),
	Index * ?GUID_COUNT + I.

call_other_entity(Guid) ->
	case find_guid(Guid) of
		{ok, Pid} -> Pid ! {rpc, "Test", self()};
		error -> ok
	end.
entity_handle(Index, Guid, CalledCount) ->
	receive
		{rpc, _RpcName, From} ->  From ! {ok, result}
	after 0 ->
		ok
	end,
	timer:sleep(1000),
	CallGuid = random_guid(Index),
	case CallGuid =:= Guid of
		true ->
			ok;
		false ->
			call_other_entity(Guid)
	end,
	case CalledCount rem 100 of
		0 ->
			%log("Called count", ["Count", CalledCount]);
			ok;
		_ ->
			ok
	end,
	entity_handle(Index, Guid, CalledCount + 1).

start_test(Index) ->
	start_test(Index, ?GUID_COUNT).

start_test(Index, Count) ->
	Seqs = lists:seq(1, Count),
	% percept2:profile("guid_router_profile.dat", [all, {callgraph, guid_router}]),
	start(),
	lists:foreach(fun(I) ->
						  Guid = Index * Count + I,
						  log("Spawn entity", ["Guid", Guid]),
						  spawn(fun() ->
										random:seed(now()),
										add_guid(Guid),
										entity_handle(Index, Guid, 0)
								end)
				  end, Seqs),
	
	{BeforeTime, 0} = statistics(context_switches),
	timer:sleep(20000),
	{AfterTime, 0} = statistics(context_switches),
	log("Context switch time", ["Time", AfterTime - BeforeTime]),
	% percept2:stop_profile(),
	% percept2:analyze(["guid_router_profile.dat"]),
	% log("Before starting web server"),
	% percept2:start_webserver(8888).
	ok.

start_simple_test() ->
	Seqs = lists:seq(1, ?GUID_COUNT),
	start(),
	lists:foreach(fun(I) ->
						 add_guid(I)
				 end, Seqs),
	gen_server:call(?ROUTER_NAME, {find_guid, 1}, infinity),
	log("add guid done"),
	statistics(wall_clock),
	lists:foreach(fun(I) ->
						  find_guid(I)
				  end, Seqs),
	{_, CostTime} = statistics(wall_clock),
	log("Simple find guid cost", ["CostTime", CostTime]).

