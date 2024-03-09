---
--- @description: 调用次数流控（支持消费者维度的调用次数流控）
--- @author: xgz
--- @date: 2023/9/11
---

local limit_local_new      = require("resty.limit.count").new
local core                 = require("apisix.core")
local common_util          = require("apisix.plugins.common.common-util")
local intercept_result     = common_util.intercept_result
local get_subscriber_id    = common_util.get_subscriber_id
local perf_log             = common_util.perf_log
local read_redis_conf      = common_util.read_redis_conf
local init_plugin_metadata = common_util.init_plugin_metadata
local ngx                  = ngx
local ngx_now              = ngx.now

local plugin_name = "limit-count-by-subscriber"
local limit_redis_cluster_new
local limit_redis_new
do
    local redis_src = "apisix.plugins.limit-count.limit-count-redis"
    limit_redis_new = require(redis_src).new

    local cluster_src = "apisix.plugins.limit-count.limit-count-redis-cluster"
    limit_redis_cluster_new = require(cluster_src).new
end
local lrucache = core.lrucache.new({
    type = 'plugin', serial_creating = true,
})

local cluster_info = {}

local schema = {
    type = "object",
    properties = {
        key = {
            type = "string",
            enum = {"remote_addr", "server_addr", "http_x_real_ip",
                    "http_x_forwarded_for", "consumer_name"},
            default = "remote_addr",
        },
        default_count = {type = "integer", exclusiveMinimum = 0},
        default_time_window = {type = "integer",  exclusiveMinimum = 0},
        scope = {
            type = "string",
            enum = {"route_id", "service_id"},
            default = "route_id",
        },
        apps = {
            type = "object",
            items = {
                type = "object",
                count = {type = "integer", exclusiveMinimum = 0},
                time_window = {type = "integer",  exclusiveMinimum = 0},
            }
        },
        rejected_code = {
            type = "integer", minimum = 200, maximum = 599, default = 429
        },
        error_interrupt = {type = "boolean", default = false},
    }
}


local _M = {
    version = 0.1,
    priority = 1888,
    name = plugin_name,
    schema = schema,
}


function _M.check_schema(conf)
    local ok, err = core.schema.check(schema, conf)
    if not ok then
        return false, err
    end

    return true
end


local function create_limit_obj(conf, ctx)
    core.log.info("create new limit-count plugin instance")

    local req_key = ctx.subscriber_id
    local item_count = 0
    local item_time_window = 0
    if conf.apps and conf.apps[req_key] ~= nil then
        item_count = conf.apps[req_key].count
        item_time_window = conf.apps[req_key].time_window
    else
        item_count = conf.default_count
        item_time_window = conf.default_time_window

    end

    if not conf.policy or conf.policy == "local" then
        return limit_local_new("plugin-" .. plugin_name, item_count,
                item_time_window)
    end

    if conf.redis_policy == "redis" then
        return limit_redis_new("plugin-" .. plugin_name,
                item_count, item_time_window, conf)
    end

    if conf.redis_policy == "redis-cluster" then
        return limit_redis_cluster_new("plugin-" .. plugin_name, item_count,
                item_time_window, conf)
    end

    return nil
end


local function gen_limit_key(ctx, conf)
    local key
    local req_key = ctx.var[conf.key]
    core.log.warn("req_key: ", req_key)
    -- 判断是否设置了订阅者级别的流控
    if conf.apps and conf.apps[req_key] ~= nil then
        local limit_key = req_key .. "#" .. conf.scope
        key = limit_key or ""
    else
        -- 没有设置订阅者级别的流控，则共用这个路由的默认流控
        key = conf.scope
    end

    core.log.warn("limit key: ", key)

    return "#" .. key .. "#" .. ctx.conf_type .. "#" .. ctx.conf_version
end


function _M.init()
    read_redis_conf(cluster_info)
end


function _M.access(conf, ctx)
    local start_time = ngx_now()
    core.log.warn("conf_version: ", ctx.conf_version)

    if conf.apps == nil and conf.default_count == nil then
        return
    end

    init_plugin_metadata(conf, cluster_info)

    local req_key = get_subscriber_id(ctx)
    if not req_key then
        return
    end
    if conf.apps and conf.apps[req_key] == nil and conf.default_count == nil then
        return
    end

    local lim, err = core.lrucache.plugin_ctx(lrucache, ctx, conf.policy, create_limit_obj, conf, ctx)

    if lim then
        local delay, remaining = lim:incoming(gen_limit_key(ctx, conf), true)
        if not delay then
            local err = remaining
            if err == "rejected" then
                return intercept_result(ctx, { errmsg = "你的访问已被拒绝，当前请求已超过该接口的调用次数限制，请稍后重试",
                                               errcode = "AGW.1430" })
            end

            core.log.error("failed to limit count: ", err)
            if conf.error_interrupt then
                return 500, {error_msg = "failed to limit count, please check the configuration: " .. err}
            end
        end
        local item_count = 0
        local item_time_window = 0
        if conf.apps and conf.apps[req_key] ~= nil then
            item_count = conf.apps[req_key].count
        else
            item_count = conf.default_count

        end
        core.response.set_header("X-RateLimit-Limit", item_count,
                "X-RateLimit-Remaining", remaining)
    else
        core.log.error("failed to fetch limit.count object: ", err)
        if conf.error_interrupt then
            return 500, {error_msg = "failed to limit count, please check the configuration: " .. err}
        end
    end

    perf_log("limit-count-by-subscriber", ngx_now() - start_time)
end


return _M
