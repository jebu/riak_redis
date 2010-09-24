{application, riak_redis,
 [
  {description, "A Redis for Riak"},
  {vsn, "1.0"},
  {modules, [
             riak_redis_app,
             riak_redis_sup,
             riak_kv_redis_backend
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, { riak_redis_app, []}},
  {env, []}
 ]}.
