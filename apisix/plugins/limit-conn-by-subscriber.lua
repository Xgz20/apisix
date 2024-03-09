---
--- @description: 并发数流控（支持消费者维度的并发数流控）
--- @author: xgz
--- @date: 2023/9/11
---

local core                 = require("apisix.core")
local deepcopy             = require("apisix.core.table").deepcopy
local apisix_plugin        = require("apisix.plugin")
local common_util          = require("apisix.plugins.common.common-util")
local get_subscriber_id    = common_util.get_subscriber_id
local intercept_result     = common_util.intercept_result
local perf_log             = common_util.perf_log
local read_redis_conf      = common_util.read_redis_conf
local init_plugin_metadata = common_util.init_plugin_metadata
local sleep                = core.sleep
local ngx                  = ngx
local ngx_now              = ngx.now
local ngx_timer_at         = ngx.timer.at


local limit_conn_local_new
local limit_conn_redis_new
do
    local local_src = "apisix.plugins.limit-conn.limit-conn-local"
    limit_conn_local_new = require(local_src).new

    local redis_src = "apisix.plugins.limit-conn.limit-conn-redis"
    limit_conn_redis_new = require(redis_src).new
end

local shdict_name = "plugin-limit-conn-by-subscriber"
if ngx.config.subsystem == "stream" then
    shdict_name = shdict_name .. "-stream"
end

local lrucache = core.lrucache.new({
    type = 'plugin', serial_creating = true,
})

local cluster_info = {}

local plugin_name = "limit-conn-by-subscriber"

local schema = {
    type = "object",
    properties = {
        default_conn = {type = "integer", exclusiveMinimum = 0},
        burst = {type = "integer",  minimum = 0},
        default_conn_delay = {type = "number", exclusiveMinimum = 0},
        only_use_default_delay = {type = "boolean", default = false},
        key = {type = "string", default = "consumer_name"},
        key_type = {type = "string",
            enum = {"var", "var_combination"},
            default = "var",
        },
        apps = { description = "apps", type = "object" },
        rejected_code = {
            type = "integer", minimum = 200, maximum = 599, default = 503
        },
        rejected_msg = {
            type = "string", minLength = 1
        },
        allow_degradation = {type = "boolean", default = true},
    },
    required = {"key"}
}

local _M = {
    version = 0.1,
    priority = 1889,
    name = plugin_name,
    schema = schema,
}


local function create_limit_obj(conf, ctx)
    core.log.info("create new limit-conn plugin instance")

    local req_key = ctx.subscriber_id
    local subscriber_conn
    if conf.apps and conf.apps[req_key] ~= nil then
        subscriber_conn = conf.apps[req_key].conn
        conf.subscriber_conn = subscriber_conn
    end
    local service_conn = conf.default_conn

    if not conf.policy or conf.policy == "local" then
        return limit_conn_local_new(shdict_name, subscriber_conn, service_conn, 0, 1)
    end

    if conf.policy == "cluster" then
        return limit_conn_redis_new("plugin-" .. plugin_name, subscriber_conn, service_conn, 0, 1, conf)
    end
end


local function gen_subscriber_limit_key(key, ctx, conf, plugin_conf_version)
    local req_key = ctx.var[conf.key]
    -- 判断是否设置了订阅者级别的流控
    if conf.apps and conf.apps[req_key] ~= nil then
        return key .. "#" .. ctx.conf_type .. "#" .. plugin_conf_version
    end

    return "NA"
end


local function gen_service_limit_key(ctx, conf, plugin_conf_version)
    return ctx.conf_type .. "#" .. plugin_conf_version
end


local function increase(conf, ctx, plugin_conf_version)
    core.log.notice("-------------------------- increase --------------------------")
    local start_time = ngx_now()

    if conf.apps == nil and conf.default_conn == nil then
        return
    end

    local req_key = get_subscriber_id(ctx)
    if not req_key then
        return
    end
    if conf.apps and conf.apps[req_key] == nil and conf.default_conn == nil then
        return
    end

    local lim, err = core.lrucache.plugin_ctx(lrucache, ctx, conf.policy, create_limit_obj, conf, ctx)

    if not lim then
        core.log.error("failed to instantiate a resty.limit.conn object: ", err)
        if conf.allow_degradation then
            return
        end
        return 500
    end

    local conf_key = conf.key
    local key
    if conf.key_type == "var_combination" then
        local err, n_resolved
        key, err, n_resolved = core.utils.resolve_var(conf_key, ctx.var)
        if err then
            core.log.error("could not resolve vars in ", conf_key, " error: ", err)
        end

        if n_resolved == 0 then
            key = nil
        end
    else
        key = ctx.var[conf_key]
    end

    if key == nil then
        core.log.warn("The value of the configured key is empty, use client IP instead")
        -- When the value of key is empty, use client IP instead
        key = ctx.var["remote_addr"]
    end

    core.log.notice("xgz【normal】limit-conn consumer key: ", key)

    local subscriber_limit_key = gen_subscriber_limit_key(key, ctx, conf, plugin_conf_version)
    local service_limit_key = gen_service_limit_key(ctx, conf, plugin_conf_version)
    core.log.notice("subscriber_limit_key: ", subscriber_limit_key)
    core.log.notice("service_limit_key: ", service_limit_key)
    local delay, err = lim:incoming_with_subscriber(subscriber_limit_key, service_limit_key, true)
    if not delay then
        if err == "rejected" then
            return intercept_result(ctx, { errmsg = "你的访问已被拒绝，当前请求已超过该接口的调用并发限制，请稍后重试",
                                           errcode = "AGW.1429" })
        end

        core.log.error("failed to limit conn【increase】: ", err)
        if conf.allow_degradation then
            return
        end
        return 500
    end

    local committed = lim:is_committed()
    core.log.notice("xgz【normal】lim:is_committed()=", committed, ", delay: ", delay)
    if committed then
        if not ctx.limit_conn then
            ctx.limit_conn = core.tablepool.fetch("plugin#limit-conn-by-subscriber", 0, 6)
        end

        core.table.insert_tail(ctx.limit_conn, lim, {subscriber_limit_key, service_limit_key},
                delay, conf.only_use_default_delay)
    end

    if delay >= 0.001 then
        core.log.alert("--- 异常情况：走到了延迟流程 ---")
        sleep(delay)
    end
    perf_log("limit-conn-by-subscriber-increase", ngx_now() - start_time)

end


local function decrease(premature, conf, ctx)
    if premature then
        return
    end

    core.log.notice("-------------------------- decrease --------------------------")

    local start_time = ngx_now()

    local limit_conn = ctx.limit_conn
    if not limit_conn then
        return
    end

    for i = 1, #limit_conn, 4 do
        local lim = limit_conn[i]
        local key = limit_conn[i + 1]
        local delay = limit_conn[i + 2]
        local use_delay =  limit_conn[i + 3]

        local latency
        if not use_delay then
            if ctx.proxy_passed then
                latency = ctx.upstream_response_time
            else
                latency = ctx.request_time - delay
            end
        end
        --core.log.notice("request latency is: ", latency)

        local conn, err = lim:leaving_with_subscriber(key[1], key[2], latency)
        if not conn then
            core.log.error("failed to record the connection leaving request【decrease】: ",
                    err)
            break
        end
    end

    core.tablepool.release("plugin#limit-conn-by-subscriber", limit_conn)
    ctx.limit_conn = nil

    perf_log("limit-conn-by-subscriber-decrease", ngx_now() - start_time)

    return
end


function _M.check_schema(conf)
    return core.schema.check(schema, conf)
end


function _M.init()
    read_redis_conf(cluster_info)
end


function _M.access(conf, ctx)
    init_plugin_metadata(conf, cluster_info)
    core.log.notice("conf_version: ", ctx.conf_version)
    local plugin_conf_version = apisix_plugin.conf_version(conf)
    core.log.notice("plugin_conf_version: ", plugin_conf_version)

    return increase(conf, ctx, plugin_conf_version)
end


function _M.log(conf, ctx)
    init_plugin_metadata(conf, cluster_info)

    local my_ctx = {}
    my_ctx.limit_conn = deepcopy(ctx.limit_conn)
    my_ctx.proxy_passed = ctx.proxy_passed
    my_ctx.upstream_response_time = ctx.var.upstream_response_time
    my_ctx.request_time = ctx.var.request_time

    local ok, err = ngx_timer_at(0, decrease, conf, my_ctx)
    if not ok then
        core.log.error("Failed to create timer: ", err)
    end

    ctx.limit_conn = nil
end


return _M