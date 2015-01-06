%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @doc The socialapp is a test application for antidote.
%%      It simulates a social networking application similar to
%%      WaltSocial and SwiftSocial
%%

-module(social).

-export([
         init/2,
         create_user/2,
         update_user/3,
         read_all/2,
         post_message/4,
         update_status/3,
         read_message/2,
         answer_friend_request/4,
         send_friend_request/3,
         read_friend_list/2,
         read_friend_requests_in/2
         ]).

-type key() :: term().

-define(SERVER, 'antidote@127.0.0.1').

%% -record (user,{profile,
%%                messages,
%%                friendlist,
%%                friendrequestsin,
%%                friendrequestsout,
%%                eventlist
%%               }
%%         )

-define(PROFILETYPE, riak_dt_lwwreg).
-define(MSGTYPE, riak_dt_orset).
-define(FRNDSTYPE, riak_dt_orset).
-define(FRNDINTYPE, riak_dt_orset).
-define(FRNDOUTTYPE, riak_dt_orset).
-define(EVENTLIST, riak_dt_orset). %% TODO: Do we need this?

%% @doc Initializes the application by setting a cookie.
init(Nodename, Cookie) ->
    case net_kernel:start([Nodename, longnames]) of
        {ok, _} ->
            erlang:set_cookie(node(), Cookie);
        {error, Reason} ->
           {error, Reason}
    end.

call_fun(Fun, Param, Server) ->
     rpc:call(Server, antidote, Fun, Param).

%% @doc create a new user
%% Input : Name of the user
%% Output : {ok, UserId}
%%           UserId is the key that can be used later to access user's profile
-spec create_user(Name::string(), Server::term()) -> {ok, UserId::key()}.
create_user(Name, Server) ->
    RandKey = get_random_key(1000),
    UserKey = RandKey, %% TODO make key unique
    Actor = self(),
    case call_fun(append, [UserKey, ?PROFILETYPE, {{assign, Name}, Actor}], Server) of
        {ok, _} ->
            {ok, UserKey};
        Result -> 
            lager:info(" ERROR : ~p", [Result]),
            {error, create_user_failed}
    end.


%% single key update
%% @doc update user's profile (currently only name)
-spec update_user(UserId::key(), Name::string(), Server::term()) -> ok.
update_user(UserId, Name, Server) ->
    Actor = self(),
    case call_fun(append, [UserId, ?PROFILETYPE, {{assign, Name}, Actor}], Server) of
        {ok, _} ->
            ok;
        _ -> {error, update_user_failed}
    end.

%% bulk_get transaction
%% @doc read everything related to a user's account
%% user's profile, friend list, wall messages etc.
-spec read_all(UserId :: key(), Server::term()) -> ok.
read_all(_UserId, _Server) ->
    ok.

%% Bulk write txn
%% @doc post a message to another user
-spec post_message(UserId::key(), Message::string(), ReceiverId::key(), Server::term()) -> ok.
post_message(UserId, Message, ReceiverId, Server) ->
    ReceiverMsgList = get_msg_key(ReceiverId),
    case call_fun(append, [ReceiverMsgList, ?MSGTYPE,
                           {{add, {Message, {from, UserId}}},
                            self()}], Server) of
        {ok, _} ->
            ok;
        _ -> {error, msg_not_sent}
    end.

%% single key write txn or bulk write txn
%% @doc update user's wall status
-spec update_status(UserId :: key(), Message::string(), Server::term()) -> ok.
update_status(UserId, Message, Server) ->
    MsgList = get_msg_key(UserId),
    case call_fun(append, [MsgList, ?MSGTYPE, {{add, Message},
                                              self()}], Server) of
        {ok, _} ->
            ok;
        _ -> {error, msg_not_sent}
    end.

read_message(UserId, Server) ->
    MsgList = get_msg_key(UserId),
    case call_fun(read, [MsgList, ?MSGTYPE], Server) of
        {ok, Msg} ->
            {ok, Msg};
        _ -> {error, read_failed}
    end.

%% write txn
%% @doc accept or rejects a friend request
-spec answer_friend_request(UserId :: key(), RequestId :: key(),
                            Accept::boolean() , Server::term()) -> ok.
answer_friend_request(UserId, RequestId, Accept, Server) ->

    case Accept of 
        true ->
            Updates = 
                [ 
                  { update, get_infrnd_key(UserId), ?FRNDINTYPE, {{remove, RequestId}, self()}},
                  { update, get_outfrnd_key(RequestId), ?FRNDOUTTYPE, {{remove, UserId}, self()}},
                  { update, get_friends_key(UserId), ?FRNDSTYPE, {{add, RequestId}, self()}},
                  { update, get_friends_key(RequestId), ?FRNDSTYPE, {{add, UserId}, self()}}
                ];
        false ->
            Updates = 
                [ 
                  { update, get_infrnd_key(UserId), ?FRNDINTYPE, {{remove, RequestId}, self()}},
                  { udpate, get_outfrnd_key(RequestId), ?FRNDOUTTYPE, {{remove, UserId}, self()}}
                ]
    end,
    case call_fun(clocksi_bulk_update, [Updates], Server) of
        {ok, _} ->
            ok;
        Reason ->
            {error, Reason}
    end.

%% write txn
%% @doc send a friend request to another user
%%   - a bulk write to update friendlist of both users
-spec send_friend_request(UserId::key(), FriendId::key(), Server::term()) -> ok.
send_friend_request(UserId, FriendId, Server) ->
    Updates = [ 
                { update, get_infrnd_key(FriendId), ?FRNDINTYPE, {{add, UserId}, self()}},
                { update, get_outfrnd_key(UserId), ?FRNDOUTTYPE, {{add, FriendId}, self()}}],
    case call_fun(clocksi_bulk_update, [Updates], Server) of
        {ok, _} ->
            ok;
        Reason ->
            {error, Reason}
    end.

%% @doc read friend list with their names
%%   - probably with interactive transaction - with one read to get userids
%%  of all friends and another read to get their names
-spec read_friend_list(UserId :: key(), Server::term()) -> ok.
read_friend_list(UserId, Server) ->
    {ok, FriendIds} = call_fun(read, [get_friends_key(UserId), ?FRNDSTYPE], Server),
    
    Ops = lists:foldl(fun(Frnd, Acc) ->
                        Acc ++ [{read, Frnd, ?PROFILETYPE}]
                      end, [], FriendIds),
    case call_fun(clocksi_execute_tx, [Ops], Server) of
        {ok, {_, Res, _}} ->
            {ok, Res};
        Reason ->
            {error, Reason}
    end.

read_friend_requests_in(UserId, Server) ->
    case call_fun(read, [get_infrnd_key(UserId), ?FRNDINTYPE], Server) of
        {ok, Ids} ->
            {ok, Ids};
        Reason ->
            {error, Reason}
    end. 
%% -------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------

get_random_key(MaxKey) ->
    _ = random:seed(now()),
    random:uniform(MaxKey).

get_msg_key(UserId) ->
    string:concat(integer_to_list(UserId), "msg").

get_infrnd_key(UserId) ->
    string:concat(integer_to_list(UserId), "infrnd").

get_outfrnd_key(UserId) ->
    string:concat(integer_to_list(UserId)  , "outfrnd").

get_friends_key(UserId) ->
    string:concat(integer_to_list(UserId), "frnds").
