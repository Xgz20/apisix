---
--- @description: 调用次数流控（支持消费者维度的调用次数流控）
--- @author: xgz
--- @date: 2023/9/11
---

local core                 = require("apisix.core")
local apisix_plugin        = require("apisix.plugin")
local common_util          = require("apisix.plugins.common.common-util")
local intercept_result     = common_util.intercept_result
local get_subscriber_id    = common_util.get_subscriber_id
local perf_log             = common_util.perf_log
local read_redis_conf      = common_util.read_redis_conf
local init_plugin_metadata = common_util.init_plugin_metadata
local ngx                  = ngx
local ngx_now              = ngx.now

local plugin_name = "limit-count-by-subscriber"

local limit_local_new
local limit_redis_new
do
    local local_src = "apisix.plugins.limit-count.limit-count-local-subscriber"
    limit_local_new = require(local_src).new

    local redis_src = "apisix.plugins.limit-count.limit-count-redis-subscriber"
    limit_redis_new = require(redis_src).new
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
    if conf.apps and conf.apps[req_key] ~= nil then
        local subscriber_limit_info = conf.apps[req_key]
        conf.subscriber_count = subscriber_limit_info.count
        conf.subscriber_time_window = subscriber_limit_info.time_window
    end

    if not conf.policy or conf.policy == "local" then
        return limit_local_new("plugin-" .. plugin_name, conf)
    end

    if conf.policy == "cluster" then
        return limit_redis_new("plugin-" .. plugin_name, conf)
    end

    return nil
end


local function gen_subscriber_limit_key(ctx, conf, plugin_conf_version)
    local req_key = ctx.var[conf.key]
    core.log.notice("req_key: ", req_key)
    -- 判断是否设置了订阅者级别的流控
    if conf.apps and conf.apps[req_key] ~= nil then
        local limit_key = req_key .. "#" .. conf.scope
        return "#" .. limit_key .. "#" .. ctx.conf_type .. "#" .. plugin_conf_version
    end

    return nil
end


local function gen_service_limit_key(ctx, conf, plugin_conf_version)
    return "#" .. conf.scope .. "#" .. ctx.conf_type .. "#" .. plugin_conf_version
end


local function parse_limit_result(limit_result)
    --core.log.notice("limit_result: ", core.json.encode(limit_result))
    local subscriber_limit_result = limit_result[1]
    local subscriber_delay = subscriber_limit_result[1]
    local subscriber_remaining = subscriber_limit_result[2]

    local service_limit_result = limit_result[2]
    local service_delay = service_limit_result[1]
    local service_remaining = service_limit_result[2]

    if not subscriber_delay then
        return subscriber_delay, subscriber_remaining
    else
        return service_delay, service_remaining
    end
end


function _M.init()
    read_redis_conf(cluster_info)
end


function _M.access(conf, ctx)
    local start_time = ngx_now()
    core.log.notice("conf_version: ", ctx.conf_version)

    if conf.apps == nil and conf.default_count == nil then
        return
    end

    local req_key = get_subscriber_id(ctx)
    if not req_key then
        return
    end
    if conf.apps and conf.apps[req_key] == nil and conf.default_count == nil then
        return
    end

    init_plugin_metadata(conf, cluster_info)

    local plugin_conf_version = apisix_plugin.conf_version(conf)
    core.log.notice("plugin_conf_version: ", plugin_conf_version)

    local lim, err = core.lrucache.plugin_ctx(lrucache, ctx, conf.policy, create_limit_obj, conf, ctx)

    local subscriber_limit_key = gen_subscriber_limit_key(ctx, conf, plugin_conf_version)
    local service_limit_key = gen_service_limit_key(ctx, conf, plugin_conf_version)
    core.log.notice("subscriber_limit_key: ", subscriber_limit_key)
    core.log.notice("service_limit_key: ", service_limit_key)
    if lim then
        local limit_result = lim:incoming_with_subscriber(subscriber_limit_key, service_limit_key, true, conf)
        local delay, remaining = parse_limit_result(limit_result)
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
        --local item_count = 0
        --local item_time_window = 0
        --if conf.apps and conf.apps[req_key] ~= nil then
        --    item_count = conf.apps[req_key].count
        --else
        --    item_count = conf.default_count
        --
        --end
        --core.response.set_header("X-RateLimit-Limit", item_count,
        --        "X-RateLimit-Remaining", remaining)
    else
        core.log.error("failed to fetch limit.count object: ", err)
        if conf.error_interrupt then
            return 500, {error_msg = "failed to limit count, please check the configuration: " .. err}
        end
    end

    perf_log("limit-count-by-subscriber", ngx_now() - start_time)
end


return _M
