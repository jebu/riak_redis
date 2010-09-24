%% -------------------------------------------------------------------
%%
%% riak_kv_redis_backend: Redis Driver for Riak
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
%% This is fully based on riak_kv_bitcask_backend
%% 
%% -------------------------------------------------------------------

-module(riak_kv_redis_backend).
-behavior(riak_kv_backend).
-author('Jebu Ittiachen <jebu@iyottasoft.com>').

-export([start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         list_bucket/2,
         fold/3,
         drop/1,
         is_empty/1,
         callback/3]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
-record(state, {pid, partition}).

start(Partition, _Config) ->
  {ok, Pid} = erldis:connect(),
  P=list_to_binary(atom_to_list(node()) ++ ":" ++ integer_to_list(Partition)),
  {ok, #state{pid=Pid, partition = P}}.

stop(_State) ->
    ok.

get(#state{partition=P, pid=Pid}, BK) ->
  case erldis:get(Pid, k2l(P,BK)) of
    nil -> {error, notfound};
    Val when is_binary(Val) -> 
      {ok, Val};
    _ ->
      {error, notfound}
  end.

put(#state{partition=P,pid=Pid}, {Bucket, _Key}=BK, Value)->
  erldis:sadd(Pid, <<"buckets:",P/binary>>,Bucket),
  erldis:set(Pid, k2l(P,BK), Value),
  ok.

delete(#state {partition=P, pid=Pid }, BK) ->
  erldis:del(Pid, k2l(P,BK)),
  ok.

list(#state {partition=P, pid=Pid}) ->
  SizeP = size(P),
  lists:map(fun(Val) ->
    <<P:SizeP/binary, $:, K1/binary>> = Val,
    [B,K] = re:split(K1, ":"),
    {B, K}
  end, erldis:keys(Pid, <<P/binary, $*>>)).

list_bucket(#state {partition=P, pid=Pid}, '_')->
  erldis:smembers(Pid, <<"buckets:",P/binary>>);  

list_bucket(State, {filter, Bucket, Fun})->
  lists:filter(Fun, list_bucket(State, Bucket));
list_bucket(#state {partition=P,  pid=Pid }, Bucket) ->
  SizeP = size(P),
  SizeB = size(Bucket),
  lists:map(fun(Val) ->
    <<P:SizeP/binary, $:, Bucket:SizeB/binary, $:, K1/binary>> = Val,
    K1
  end, erldis:keys(Pid, <<P/binary, $:, Bucket/binary,$:, $*>>)).

fold(#state {partition=P, pid=Pid}, Fun0, Acc0) ->
  SizeP = size(P),
  lists:foldl(fun(Key, Acc) ->
    <<P:SizeP/binary, $:, K1/binary>> = Key,
    case erldis:get(Pid, Key) of
      Val when is_binary(Val) ->
        [B,K] = re:split(K1, ":"),
        Fun0({B,K}, Val, Acc);
      _ ->
        Acc
    end
  end, Acc0, erldis:keys(Pid, <<P/binary, $:, $*>>)).

drop(_State) ->
  ok.

is_empty(State) ->
    F = fun(_K, _V, Acc) ->
          Acc + 1
        end,
    case fold(State, F, 0) of
      0 -> true;
      _ -> false
    end.
%% Ignore callbacks for other backends so multi backend works
callback(_State, _Ref, _Msg) ->
    ok.

%% ===================================================================
%% Internal functions
%% ===================================================================
get_env(App, ConfParam, Default) ->
  case application:get_env(App, ConfParam) of
    {ok, Val} -> Val;
    _ -> Default
  end.
%
k2l(P,{B, V})->
  <<P/binary, $:, B/binary, $:, V/binary>>.
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
standard_test(BackendMod, Config) ->
    {ok, S} = BackendMod:start(42, Config),
    ?assertEqual(ok, BackendMod:put(S,{<<"b1">>,<<"k1">>},<<"v1">>)),
    ?assertEqual(ok, BackendMod:put(S,{<<"b2">>,<<"k2">>},<<"v2">>)),
    ?assertEqual({ok,<<"v2">>}, BackendMod:get(S,{<<"b2">>,<<"k2">>})),
    ?assertEqual({error, notfound}, BackendMod:get(S, {<<"b1">>,<<"k3">>})),
    ?assertEqual([{<<"b1">>,<<"k1">>},{<<"b2">>,<<"k2">>}],
                 lists:sort(BackendMod:list(S))),
    ?assertEqual([<<"k2">>], BackendMod:list_bucket(S, <<"b2">>)),
    ?assertEqual([<<"k1">>], BackendMod:list_bucket(S, <<"b1">>)),
    ?assertEqual([<<"k1">>], BackendMod:list_bucket(
                               S, {filter, <<"b1">>, fun(_K) -> true end})),
    ?assertEqual([], BackendMod:list_bucket(
                       S, {filter, <<"b1">>, fun(_K) -> false end})),
    BucketList = BackendMod:list_bucket(S, '_'),
    ?assert(lists:member(<<"b1">>, BucketList)),
    ?assert(lists:member(<<"b2">>, BucketList)),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b2">>,<<"k2">>})),
    ?assertEqual({error, notfound}, BackendMod:get(S, {<<"b2">>, <<"k2">>})),
    ?assertEqual([{<<"b1">>, <<"k1">>}], BackendMod:list(S)),
    Folder = fun(K, V, A) -> [{K,V}|A] end,
    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>}], BackendMod:fold(S, Folder, [])),
    ?assertEqual(ok, BackendMod:put(S,{<<"b3">>,<<"k3">>},<<"v3">>)),
    ?assertEqual([{{<<"b1">>,<<"k1">>},<<"v1">>},
                  {{<<"b3">>,<<"k3">>},<<"v3">>}], lists:sort(BackendMod:fold(S, Folder, []))),
    ?assertEqual(false, BackendMod:is_empty(S)),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b1">>,<<"k1">>})),
    ?assertEqual(ok, BackendMod:delete(S,{<<"b3">>,<<"k3">>})),
    ?assertEqual(true, BackendMod:is_empty(S)),
    ok = BackendMod:stop(S).

simple_test() ->
    ?assertCmd("rm -rf test/tokyo-cabinet-backend"),
    application:load(riak_tokyo_cabinet),
    application:set_env(riak_tokyo_cabinet, data_root, "test/tokyo_cabinet-backend"),
    standard_test(?MODULE, []).

-endif.
