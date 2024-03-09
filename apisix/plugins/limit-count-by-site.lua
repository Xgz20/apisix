---
--- @description: 支持站点API调用次数流控
--- @author: xgz
--- @date: 2024/1/2
---

local core                 = require("apisix.core")
local apisix_plugin        = require("apisix.plugin")
local common_util          = require("apisix.plugins.common.common-util")
local intercept_result     = common_util.intercept_result
local perf_log             = common_util.perf_log
local read_redis_conf      = common_util.read_redis_conf
local init_plugin_metadata = common_util.init_plugin_metadata
local ngx                  = ngx
local ngx_now              = ngx.now


local plugin_name = "limit-count-by-site"

local limit_local_new
local limit_redis_new
local limit_redis_cluster_new
do
    local local_src = "apisix.plugins.limit-count.limit-count-local"
    limit_local_new = require(local_src).new

    local redis_src = "apisix.plugins.limit-count.limit-count-redis"
    limit_redis_new = require(redis_src).new

    local cluster_src = "apisix.plugins.limit-count.limit-count-redis-cluster"
    limit_redis_cluster_new = require(cluster_src).new
end
local lrucache = core.lrucache.new({
    type = 'plugin', serial_creating = true,
})
local group_conf_lru = core.lrucache.new({
    type = 'plugin',
})


local cluster_info = {}

local schema = {
    type = "object",
    properties = {
        count = {type = "integer", exclusiveMinimum = 0},
        time_window = {type = "integer",  exclusiveMinimum = 0, default = 1},
        group = {type = "string"},
        key = {type = "string", default = "service_name"},
        key_type = {type = "string",
                    enum = {"var", "var_combination", "constant"},
                    default = "var",
        },
        rejected_code = {
            type = "integer", minimum = 200, maximum = 599, default = 429
        },
        rejected_msg = {
            type = "string", minLength = 1
        },
        allow_degradation = {type = "boolean", default = true},
        show_limit_quota_header = {type = "boolean", default = false}
    },
    required = {"count", "group"}
}


local _M = {
    version = 0.1,
    priority = 1776,
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


local function gen_limit_key(conf, ctx, key)
    if conf.group then
        local plugin_conf_version = apisix_plugin.conf_version(conf)
        core.log.notice("plugin_conf_version: ", plugin_conf_version)
        return '#' .. conf.group .. '#' .. key .. '#' .. plugin_conf_version
    end

    -- here we add a separator ':' to mark the boundary of the prefix and the key itself
    -- Here we use plugin-level conf version to prevent the counter from being resetting
    -- because of the change elsewhere.
    -- A route which reuses a previous route's ID will inherits its counter.
    local new_key = ctx.conf_type .. ctx.conf_id .. '#' .. apisix_plugin.conf_version(conf)
            .. '#' .. key
    if conf._vid then
        -- conf has _vid means it's from workflow plugin, add _vid to the key
        -- so that the counter is unique per action.
        return new_key .. '#' .. conf._vid
    end

    return new_key
end


local function create_limit_obj(conf, ctx)
    core.log.info("create new limit-count plugin instance")
    if not conf.policy or conf.policy == "local" then
        return limit_local_new("plugin-" .. plugin_name, conf.count,
                conf.time_window)
    end

    if conf.redis_policy == "redis" then
        return limit_redis_new("plugin-" .. plugin_name,
                conf.count, conf.time_window, conf)
    end

    if conf.redis_policy == "redis-cluster" then
        return limit_redis_cluster_new("plugin-" .. plugin_name, conf.count,
                conf.time_window, conf)
    end

    return nil
end


local function gen_limit_obj(conf, ctx)
    if conf.group then
        return lrucache(conf.group, "", create_limit_obj, conf)
    end

    local extra_key
    if conf._vid then
        extra_key = conf.policy .. '#' .. conf._vid
    else
        extra_key = conf.policy
    end

    return core.lrucache.plugin_ctx(lrucache, ctx, extra_key, create_limit_obj, conf)
end


local function rate_limit(conf, ctx)
    local start_time = ngx_now()
    core.log.notice("conf_version: ", ctx.conf_version)

    local lim, err = gen_limit_obj(conf, ctx)

    if not lim then
        core.log.error("failed to fetch limit.count object: ", err)
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
    elseif conf.key_type == "constant" then
        key = conf_key
    else
        key = ctx.var[conf_key]
    end

    if key == nil then
        core.log.notice("The value of the configured key is empty, use client IP instead")
        -- When the value of key is empty, use client IP instead
        key = ctx.var["remote_addr"]
    end

    key = gen_limit_key(conf, ctx, key)
    core.log.notice("limit key: ", key)

    local delay, remaining, reset = lim:incoming(key, true, conf)
    if not delay then
        local err = remaining
        if err == "rejected" then
            -- show count limit header when rejected
            if conf.show_limit_quota_header then
                core.response.set_header("X-RateLimit-Limit", conf.count,
                        "X-RateLimit-Remaining", 0,
                        "X-RateLimit-Reset", reset)
            end

            return intercept_result(ctx, { errmsg = "你的访问已被拒绝，当前请求已超过该接口的调用次数限制，请稍后重试",
                                           errcode = "AGW.1430" })
        end

        core.log.error("failed to limit count: ", err)
        if conf.allow_degradation then
            return
        end
        return 500, {error_msg = "failed to limit count"}
    end

    if conf.show_limit_quota_header then
        core.response.set_header("X-RateLimit-Limit", conf.count,
                "X-RateLimit-Remaining", remaining,
                "X-RateLimit-Reset", reset)
    end

    perf_log("limit-count-by-site", ngx_now() - start_time)
end


function _M.init()
    read_redis_conf(cluster_info)
end


function _M.access(conf, ctx)
    init_plugin_metadata(conf, cluster_info)

    return rate_limit(conf, ctx)
end


return _M