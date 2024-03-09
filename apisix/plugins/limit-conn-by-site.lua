---
--- @description: 支持站点API并发数流控
--- @author: xgz
--- @date: 2024/01/04
---

local core                 = require("apisix.core")
local deepcopy             = require("apisix.core.table").deepcopy
local apisix_plugin        = require("apisix.plugin")
local common_util          = require("apisix.plugins.common.common-util")
local intercept_result     = common_util.intercept_result
local perf_log             = common_util.perf_log
local read_redis_conf      = common_util.read_redis_conf
local init_plugin_metadata = common_util.init_plugin_metadata
local limit_conn_new       = require("resty.limit.conn").new
local sleep                = core.sleep
local ngx                  = ngx
local ngx_now              = ngx.now
local ngx_timer_at         = ngx.timer.at


local plugin_name = "limit-conn-by-site"

local limit_conn_redis_new
do
    local redis_src = "apisix.plugins.limit-conn.limit-conn-redis"
    limit_conn_redis_new = require(redis_src).new
end

local shdict_name = "plugin-" .. plugin_name
if ngx.config.subsystem == "stream" then
    shdict_name = shdict_name .. "-stream"
end

local lrucache = core.lrucache.new({
    type = 'plugin', serial_creating = true,
})

local cluster_info = {}


local schema = {
    type = "object",
    properties = {
        conn = {type = "integer", exclusiveMinimum = 0},
        burst = {type = "integer",  minimum = 0},
        group = {type = "string"},
        default_conn_delay = {type = "number", exclusiveMinimum = 0},
        only_use_default_delay = {type = "boolean", default = false},
        key = {type = "string", default = "service_name"},
        key_type = {type = "string",
                    enum = {"var", "var_combination"},
                    default = "var",
        },
        rejected_code = {
            type = "integer", minimum = 200, maximum = 599, default = 503
        },
        rejected_msg = {
            type = "string", minLength = 1
        },
        allow_degradation = {type = "boolean", default = true},
    },
    required = {"conn", "group"}
}

local _M = {
    version = 0.1,
    priority = 1777,
    name = plugin_name,
    schema = schema,
}


function _M.check_schema(conf)
    return core.schema.check(schema, conf)
end


local function create_limit_obj(conf, ctx)
    core.log.info("create new limit-conn plugin instance")

    if not conf.policy or conf.policy == "local" then
        return limit_conn_new(shdict_name, conf.conn, 0, 1)
    end

    if not conf.policy or conf.policy == "cluster" then
        return limit_conn_redis_new("plugin-" .. plugin_name, 0, 0, 0, 1, conf)
    end
end


local function gen_limit_key(key, ctx, conf)
    if conf.group then
        local plugin_conf_version = apisix_plugin.conf_version(conf)
        core.log.notice("plugin_conf_version: ", plugin_conf_version)
        return conf.group .. '#' .. key .. '#' .. plugin_conf_version
    end

    return key .. "#" .. ctx.conf_type .. "#" .. ctx.conf_version
end


local function increase(conf, ctx)
    local start_time = ngx_now()
    core.log.notice("conf_version: ", ctx.conf_version)

    local lim, err = core.lrucache.plugin_ctx(lrucache, ctx, conf.policy, create_limit_obj, conf, ctx)

    if not lim then
        core.log.error("failed to instantiate a resty.limit.conn object: ", err)
        if conf.allow_degradation then
            return
        end
        return 500
    end

    -- 优先获取service_name作为基础key
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
        core.log.notice("The value of the configured key is empty, use client IP instead")
        -- When the value of key is empty, use client IP instead
        key = ctx.var["remote_addr"]
    end

    key = gen_limit_key(key, ctx, conf)
    core.log.notice("limit-conn key: ", key)

    local delay, err = lim:incoming(key, true)
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
    core.log.notice("limit-conn:is_committed()=", committed)
    if committed then
        if not ctx.limit_conn then
            ctx.limit_conn = core.tablepool.fetch("plugin#limit-conn-by-site", 0, 6)
        end

        core.table.insert_tail(ctx.limit_conn, lim, key, delay, conf.only_use_default_delay)
    end

    if delay >= 0.001 then
        sleep(delay)
    end
    perf_log("limit-conn-by-site-increase", ngx_now() - start_time)

end


local function decrease(premature, conf, ctx)
    if premature then
        return
    end

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
        core.log.info("request latency is ", latency) -- for test

        local conn, err = lim:leaving(key, latency)
        if not conn then
            core.log.error("failed to record the connection leaving request【decrease】: ",
                    err)
            break
        end
    end

    core.tablepool.release("plugin#limit-conn-by-site", limit_conn)
    ctx.limit_conn = nil

    perf_log("limit-conn-by-site-decrease", ngx_now() - start_time)

    return
end


function _M.init()
    read_redis_conf(cluster_info)
end


function _M.access(conf, ctx)
    init_plugin_metadata(conf, cluster_info)

    return increase(conf, ctx)
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
