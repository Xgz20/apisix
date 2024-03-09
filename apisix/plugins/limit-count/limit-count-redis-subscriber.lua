---
--- @description: 集群场景，实现订阅者和服务级别流控两次校验
--- @author: xgz
--- @date: 2024/1/10
---

local core = require("apisix.core")
local redis_cli = require("apisix.plugins.common.common-util").redis_cli
local setmetatable = setmetatable
local tostring = tostring

local _M = {}

local mt = {
    __index = _M
}


local script = core.string.compress_script([=[
    local ttl = redis.call('ttl', KEYS[1])
    if ttl < 0 then
        redis.call('set', KEYS[1], ARGV[1] - 1, 'EX', ARGV[2])
        return {ARGV[1] - 1, ARGV[2]}
    end
    return {redis.call('incrby', KEYS[1], -1), ttl}
]=])


function _M.new(plugin_name, conf)
    local self = {
        subscriber_count = conf.subscriber_count,
        subscriber_time_window = conf.subscriber_time_window,
        service_count = conf.default_count,
        service_time_window = conf.default_time_window,
        conf = conf,
        plugin_name = plugin_name
    }

    return setmetatable(self, mt)
end


local function parse_limit_result(redis_result)
    if not redis_result then
        core.log.error("未知的异常场景")
        return { "UNKNOWN", "UNKNOWN", "UNKNOWN" }
    end

    local remaining = redis_result[1]
    local ttl = redis_result[2]
    local limit_result
    if remaining < 0 then
        limit_result = { nil, "rejected", ttl }
    else
        limit_result = { 0, remaining, ttl }
    end
    return limit_result
end


function _M.incoming_with_subscriber(self, subscriber_key, service_key, commit, conf)
    local ttl = 0
    local init_limit_result = { "NA", "NA", "NA" }

    local red, err = redis_cli(conf)
    if not red then
        return { { nil, err, ttl }, init_limit_result }
    end

    local subscriber_count = conf.subscriber_count
    local subscriber_time_window = conf.subscriber_time_window
    local service_count = conf.default_count
    local service_time_window = conf.default_time_window

    local subscriber_limit_key = self.plugin_name .. tostring(subscriber_key)
    local service_limit_key = self.plugin_name .. tostring(service_key)

    red:init_pipeline()
    if subscriber_count and subscriber_count > 0 then
        red:eval(script, 1, subscriber_limit_key, subscriber_count, subscriber_time_window)
    end

    if service_count and service_count > 0 then
        red:eval(script, 1, service_limit_key, service_count, service_time_window)
    end

    local res, err = red:commit_pipeline()
    if not res then
        core.log.error("err: ", err)
        return { { nil, err, ttl }, init_limit_result }
    end
    --core.log.notice("result: ", core.json.encode(res))

    if conf.redis_policy == "redis" then
        local ok, err = red:set_keepalive(10000, 100)
        if not ok then
            return { { nil, err, ttl }, init_limit_result }
        end
    end

    local subscriber_limit_result = init_limit_result
    local service_limit_result = init_limit_result
    if subscriber_count and subscriber_count > 0 then
        subscriber_limit_result = parse_limit_result(res[1])

        if service_count and service_count > 0 then
            service_limit_result = parse_limit_result(res[2])
        else
            service_limit_result = init_limit_result
        end
    else
        subscriber_limit_result = init_limit_result

        if service_count and service_count > 0 then
            service_limit_result = parse_limit_result(res[1])
        else
            -- 正常应该不会走到这种场景
            service_limit_result = init_limit_result
        end
    end
    
    return { subscriber_limit_result, service_limit_result }
end

return _M