---
--- @description: 服务熔断
--- @author: 武猛 mengwu10
--- @date: 2023/8/21
--- @revision: xgz，2023/11/13，新增支持集群方式
---

local core                 = require("apisix.core")
local common_util          = require("apisix.plugins.common.common-util")
local intercept_result     = common_util.intercept_result
local redis_cli            = common_util.redis_cli
local read_redis_conf      = common_util.read_redis_conf
local init_plugin_metadata = common_util.init_plugin_metadata
local ngx                  = ngx
local null                 = ngx.null
local ngx_time             = ngx.time
local ngx_timer_at         = ngx.timer.at
local tonumber             = tonumber
local error                = error
local ipairs               = ipairs
local str_format           = string.format

local plugin_name = "service-breaker"

local shared_buffer = ngx.shared["plugin-" .. plugin_name]
if not shared_buffer then
    error("failed to get ngx.shared dict when load plugin " .. plugin_name)
end

local cluster_info = {}

local schema = {
    type = "object",
    properties = {
        break_response_code = {
            type = "integer",
            minimum = 200,
            maximum = 599,
        },
        break_response_body = {
            type = "string"
        },
        break_response_headers = {
            type = "array",
            items = {
                type = "object",
                properties = {
                    key = {
                        type = "string",
                        minLength = 1
                    },
                    value = {
                        type = "string",
                        minLength = 1
                    }
                },
                required = { "key", "value" },
            }
        },
        breaker_sec = {
            type = "integer",
            default = 300
        },
        unhealthy = {
            type = "object",
            properties = {
                http_statuses = {
                    type = "array",
                    minItems = 1,
                    items = {
                        type = "integer",
                        minimum = 500,
                        maximum = 599,
                    },
                    uniqueItems = true,
                    default = { 500 }
                },
                failures = {
                    type = "integer",
                    minimum = 1,
                    default = 3,
                }
            },
            default = { http_statuses = { 500 }, failures = 3 }
        },
        healthy = {
            type = "object",
            properties = {
                http_statuses = {
                    type = "array",
                    minItems = 1,
                    items = {
                        type = "integer",
                        minimum = 200,
                        maximum = 499,
                    },
                    uniqueItems = true,
                    default = { 200 }
                },
                successes = {
                    type = "integer",
                    minimum = 1,
                    default = 3,
                }
            },
            default = { http_statuses = { 200 }, successes = 3 }
        },
    },
    required = { "breaker_sec", "break_response_code" },
}

local _M = {
    version = 0.1,
    name = plugin_name,
    priority = 1005,
    schema = schema,
}


function _M.check_schema(conf)
    return core.schema.check(schema, conf)
end


local function gen_key(ctx, flag)
    local key_table = {}
    key_table[1] = "plugin-" .. plugin_name
    key_table[2] = flag
    key_table[3] = core.request.get_host(ctx) .. ctx.var.uri
    return core.table.concat(key_table, "")
end

local function gen_healthy_key(ctx)
    return gen_key(ctx, "-healthy-")
end

local function gen_unhealthy_key(ctx)
    return gen_key(ctx, "-unhealthy-")
end

local function gen_lasttime_key(ctx)
    return gen_key(ctx, "-unhealthy-lasttime-")
end


local function get_opt(key, conf, red)
    if conf.policy == "local" then
        return shared_buffer:get(key)
    else
        local new_red, err = red or redis_cli(conf)
        if not new_red then
            core.log.error("create redis-cli failed, reason =>", err)
            return nil, err
        end
        return new_red:get(key), nil
    end
end


local set_script = core.string.compress_script([=[
    return redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2])
]=])

local function set_opt(key, value, expire_time, conf, red)
    if conf.policy == "local" then
        shared_buffer:set(key, value, expire_time)
    else
        local new_red, err = red or redis_cli(conf)
        if not new_red then
            core.log.error("create redis-cli failed, reason =>", err)
            return nil, err
        end

        core.log.info(str_format("key=%s, value=%s, expire_time=%s", key, value, expire_time))
        local res = new_red:eval(set_script, 1, key, value, expire_time)
        core.log.info("set opt result =>", res)
    end
end


local function incr_opt(key, conf, red)
    if conf.policy == "local" then
        return shared_buffer:incr(key, 1, 0)
    else
        local new_red, err = red or redis_cli(conf)
        if not new_red then
            core.log.error("create redis-cli failed, reason =>", err)
            return nil, err
        end
        return new_red:incr(key)
    end
end


local function delete_opt(key, conf, red)
    if conf.policy == "local" then
        shared_buffer:delete(key)
    else
        local new_red, err = red or redis_cli(conf)
        if not new_red then
            core.log.error("create redis-cli failed, reason =>", err)
            return nil, err
        end
        new_red:del(key)
    end
end


function _M.init()
    read_redis_conf(cluster_info)
end


function _M.access(conf, ctx)
    init_plugin_metadata(conf, cluster_info)

    local red
    if conf.policy ~= "local" then
        red = redis_cli(conf)
    end

    local unhealthy_key = gen_unhealthy_key(ctx)
    -- 1、检查不健康计数情况
    local unhealthy_count, err = get_opt(unhealthy_key, conf, red)
    if err then
        core.log.warn("failed to get unhealthy_key: ",
                unhealthy_key, " err: ", err)
        return
    end

    if not unhealthy_count then
        return
    end

    -- 2、检查是否存在熔断时间戳（达到不健康次数阈值时生成）
    local lasttime_key = gen_lasttime_key(ctx)
    local lasttime, err = get_opt(lasttime_key, conf, red)
    core.log.info("lasttime type: ", type(lasttime), ", lasttime value: ", lasttime)
    if err then
        core.log.warn("failed to get lasttime_key: ",
                lasttime_key, " err: ", err)
        return
    end

    if not lasttime or lasttime == null then
        return
    end

    local breaker_time = conf.breaker_sec
    local now_time = ngx_time()
    core.log.warn("time diff(s): ", tonumber(lasttime) + breaker_time - now_time)

    -- 3、检查是否还在上次熔断时间窗口内（触发熔断）
    if tonumber(lasttime) + breaker_time >= now_time then
        if conf.break_response_body then
            if conf.break_response_headers then
                for _, value in ipairs(conf.break_response_headers) do
                    local val = core.utils.resolve_var(value.value, ctx.var)
                    core.response.add_header(value.key, val)
                end
            end
            return conf.break_response_code, conf.break_response_body
        end
        return intercept_result(ctx, { errmsg = "当前服务已熔断", errcode = "AGW.1504" })
    end

    return
end


local function _log(premature, conf, upstream_status, unhealthy_key, healthy_key, lasttime_key)
    --core.log.warn("xgz【conf】=>", core.json.encode(conf, true))
    if premature then
        return
    end

    local red
    if conf.policy ~= "local" then
        red = redis_cli(conf)
    end

    -- 1、检查本次请求是否满足"不健康"条件
    if core.table.array_find(conf.unhealthy.http_statuses, upstream_status) then
        local unhealthy_count, err = incr_opt(unhealthy_key, conf, red)
        if err then
            core.log.warn("failed to incr unhealthy_key: ", unhealthy_key,
                    " err: ", err)
        end
        core.log.warn("unhealthy_key: ", unhealthy_key, " count: ", unhealthy_count)

        -- 1.1 清除健康计数器
        delete_opt(healthy_key, conf, red)

        -- 1.2 判断是否超过不健康次数阈值
        if unhealthy_count % conf.unhealthy.failures == 0 then
            set_opt(lasttime_key, ngx_time(), conf.breaker_sec, conf, red)
            core.log.warn("update unhealthy_key: ", unhealthy_key, " to ", unhealthy_count)
        end

        return
    end

    -- 2、检查本次请求是否满足"健康"条件
    if not core.table.array_find(conf.healthy.http_statuses, upstream_status) then
        return
    end

    -- 3、检查是否存在"不健康"计数器
    local unhealthy_count, err = get_opt(unhealthy_key, conf, red)
    if err then
        core.log.warn("failed to `get` unhealthy_key: ", unhealthy_key,
                " err: ", err)
    end

    if not unhealthy_count then
        return
    end

    -- 4、"健康"计数器 +1
    local healthy_count, err = incr_opt(healthy_key, conf, red)
    if err then
        core.log.warn("failed to `incr` healthy_key: ", healthy_key,
                " err: ", err)
    end

    -- 5、检查"健康"计数器是否达到阈值
    if healthy_count >= conf.healthy.successes then
        core.log.info("change to normal, ", healthy_key, " ", healthy_count)
        delete_opt(lasttime_key, conf, red)
        delete_opt(unhealthy_key, conf, red)
        delete_opt(healthy_key, conf, red)
    end

    return
end


function _M.log(conf, ctx)
    init_plugin_metadata(conf, cluster_info)

    local unhealthy_key = gen_unhealthy_key(ctx)
    local healthy_key = gen_healthy_key(ctx)
    local lasttime_key = gen_lasttime_key(ctx)
    local upstream_status = core.response.get_upstream_status(ctx)

    if not upstream_status then
        return
    end

    local ok, err = ngx_timer_at(0, _log, conf, upstream_status, unhealthy_key, healthy_key, lasttime_key)
    if not ok then
        core.log.error("Failed to create timer: ", err)
    end

end

return _M
