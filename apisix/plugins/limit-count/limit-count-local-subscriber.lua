---
--- @description: 实现订阅者和服务级别流控两次校验
--- @author: xgz
--- @date: 2024/1/10
---

local limit_local_new = require("resty.limit.count").new
local ngx = ngx
local ngx_time = ngx.time
local setmetatable = setmetatable
local core = require("apisix.core")

local _M = {}

local mt = {
    __index = _M
}


local function set_endtime(self, key, time_window)
    -- set an end time
    local end_time = ngx_time() + time_window
    -- save to dict by key
    local success, err = self.dict:set(key, end_time, time_window)

    if not success then
        core.log.error("dict set key ", key, " error: ", err)
    end

    local reset = time_window
    return reset
end


local function read_reset(self, key)
    -- read from dict
    local end_time = (self.dict:get(key) or 0)
    local reset = end_time - ngx_time()
    if reset < 0 then
        reset = 0
    end
    return reset
end


function _M.new(plugin_name, conf)
    local subscriber_count = conf.subscriber_count
    local service_count = conf.default_count
    local subscriber_limit_count
    local service_limit_count
    if subscriber_count and subscriber_count > 0 then
        subscriber_limit_count = limit_local_new(plugin_name, subscriber_count, conf.subscriber_time_window)
    end
    if service_count and service_count > 0 then
        service_limit_count = limit_local_new(plugin_name, service_count, conf.default_time_window)
    end
    local self = {
        subscriber_limit_count = subscriber_limit_count,
        service_limit_count = service_limit_count,
        dict = ngx.shared["plugin-limit-count-reset-header"]
    }

    return setmetatable(self, mt)
end


function _M.incoming(self, key, commit, conf)
    local delay, remaining = self.limit_count:incoming(key, commit)
    core.log.notice("key=", key, ", delay=", delay, ", remaining=", remaining)
    local reset = 0
    if not delay then
        return delay, remaining, reset
    end

    if remaining == conf.count - 1 then
        reset = set_endtime(self, key, conf.time_window)
    else
        reset = read_reset(self, key)
    end

    return delay, remaining, reset
end


function _M.incoming_with_subscriber(self, subscriber_key, service_key, commit, conf)
    local subscriber_limit_result = {"NA", "NA", "NA"}
    local service_limit_result = {"NA", "NA", "NA"}

    local subscriber_count = conf.subscriber_count
    local subscriber_time_window = conf.subscriber_time_window
    local service_count = conf.default_count
    local service_time_window = conf.default_time_window

    if subscriber_count and subscriber_count > 0 then
        self.limit_count = self.subscriber_limit_count
        subscriber_limit_result = { _M.incoming(self, subscriber_key, commit,
                { count = subscriber_count, time_window = subscriber_time_window }) }
        --core.log.notice("subscriber_limit_result: ", core.json.encode(subscriber_limit_result))
    end

    if service_count and service_count > 0 then
        self.limit_count = self.service_limit_count
        service_limit_result = { _M.incoming(self, service_key, commit,
                { count = service_count, time_window = service_time_window }) }
        --core.log.notice("service_limit_result: ",  core.json.encode(service_limit_result))
    end

    return { subscriber_limit_result, service_limit_result }
end

return _M