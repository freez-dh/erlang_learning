-module(guid_router).

-compile(export_all).

-record(router_state, {local_guid_set, remote_guid_node}).

-behavior(gen_server).

-define(ROUTER_NAME, router).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, code_change/3, terminate/2]).

start() ->
	gen_server:start({local, ?ROUTER_NAME}, ?MODULE, [], []).

% ==============================gen_server callbacks=====================
init(_Args) ->
	{ok, #router_state{local_guid_set= dict:new(), remote_guid_node = dict:new()}}.

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

handle_cast({remove_guid, Guid}, State) ->
	LocalGuidSet = State#router_state.local_guid_set,
	NewLocalGuidSet = sets:erase(Guid, LocalGuidSet),
	notify_other_nodes_router({notify_remove_guid, Guid, node()}),
	{noreply, State#router_state{local_guid_set = NewLocalGuidSet}};

handle_cast({remote_find_guid, Guid, FromNode, Ctx}, State) ->
	LocalGuidSet = State#router_state.local_guid_set,
	Result = dict:fetch(Guid, LocalGuidSet),
	gen_server:cast({?ROUTER_NAME, FromNode}, {remote_reply_guid, Result, Ctx}),
	{noreply, State};

handle_cast({remote_reply_guid, ReplyInfo, CallFrom}, State) ->
	gen_server:reply(CallFrom, ReplyInfo),
	State.

try_find_guid_remote(Guid, From, State) ->
	RemoteGuidNode = State#router_state.remote_guid_node,
	case dict:fetch(Guid, RemoteGuidNode) of
		{ok, Node} ->
			gen_server:cast({?ROUTER_NAME, Node}, {remote_find_guid, Guid, node(), From}),
			{noreply, State};
		error ->
			{reply, error, State}
	end.

handle_call({find_guid, Guid}, From, State) ->
	LocalGuidSet = State#router_state.local_guid_set,
	case dict:fetch(Guid, LocalGuidSet) of
		{ok, Pid} ->
			{reply, {ok, Pid}, State};
		false ->
			try_find_guid_remote(Guid, From, State)
	end.

handle_info(_Info, State) ->
	{noreply, State}.

code_change(_OldVersion, State, _Extra) ->
	{ok, State}.

terminate(_Reason, _State) ->
	ok.


% ================================export apis===========================
