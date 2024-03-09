---
--- @description: 单机部署场景，支持并发数流控双维度校验
--- @author: xgz
--- @date: 2024/1/11
---

local core = require("apisix.core")
local setmetatable = setmetatable
local math = require("math")
local floor = math.floor
local ngx_shared = ngx.shared
local assert = assert


local _M = {
    _VERSION = '1.0.0'
}


local mt = {
    __index = _M
}


function _M.new(dict_name, subscriber_conn, service_conn, burst, default_conn_delay)
    local dict = ngx_shared[dict_name]
    if not dict then
        return nil, "shared dict not found"
    end

    local self = {
        dict = dict,
        subscriber_max = subscriber_conn,
        service_max = service_conn,
        burst = burst,
        unit_delay = default_conn_delay
    }

    return setmetatable(self, mt)
end


function _M.incoming(self, key, commit)
    local dict = self.dict
    local max = self.max

    self.committed = false

    local conn, err
    if commit then
        conn, err = dict:incr(key, 1, 0)
        core.log.notice("xgz【normal】【key = ", key, "】incoming: max=", max,
                ", burst=", self.burst, ", current conn=", conn)
        if not conn then
            return nil, err
        end

        if conn > max + self.burst then
            conn, err = dict:incr(key, -1)
            if not conn then
                return nil, err
            end
            return nil, "rejected"
        end
        self.committed = true

    else
        conn = (dict:get(key) or 0) + 1
        if conn > max + self.burst then
            return nil, "rejected"
        end
    end

    if conn > max then
        -- make the excessive connections wait
        return self.unit_delay * floor((conn - 1) / max), conn
    end

    -- we return a 0 delay by default
    return 0, conn
end


local function handle_limit_conn(count_func, self, subscriber_limit_key, service_limit_key, arg)
    local subscriber_max = self.subscriber_max
    local service_max = self.service_max

    local subscriber_limit_exist = false
    local service_limit_exist = false
    if subscriber_max and subscriber_max > 0 then
        subscriber_limit_exist = true
    end
    if service_max and service_max > 0 then
        service_limit_exist = true
    end

    if subscriber_limit_exist and not service_limit_exist then
        self.max = subscriber_max
        return true, count_func(self, subscriber_limit_key, arg)
    end

    if not subscriber_limit_exist and service_limit_exist then
        self.max = service_max
        return true, count_func(self, service_limit_key, arg)
    end

    if not subscriber_limit_exist and not service_limit_exist then
        core.log.alert("两种维度流控都不存在，请检查插件元数据下发是否有问题！！！")
        return true, nil, "两种维度流控都不存在，请检查插件元数据下发是否有问题"
    end

    return false
end


function _M.incoming_with_subscriber(self, subscriber_limit_key, service_limit_key, commit)
    local finished, delay, conn = handle_limit_conn(_M.incoming, self, subscriber_limit_key, service_limit_key, commit)
    if finished then
        return delay, conn
    end

    core.log.notice("===> 存在两种维度（订阅者和服务）流控策略")

    local dict = self.dict
    local subscriber_max = self.subscriber_max
    local service_max = self.service_max
    self.committed = false

    local subscriber_incoming_result, service_incoming_result, err
    if commit then
        subscriber_incoming_result, err = dict:incr(subscriber_limit_key, 1, 0)
        core.log.notice("xgz【normal】【key = ", subscriber_limit_key, "】incoming: max=", subscriber_max,
                ", burst=", self.burst, ", current conn=", subscriber_incoming_result)
        if not subscriber_incoming_result then
            return nil, err
        end

        service_incoming_result, err = dict:incr(service_limit_key, 1, 0)
        core.log.notice("xgz【normal】【key = ", service_limit_key, "】incoming: max=", service_max,
                ", burst=", self.burst, ", current conn=", service_incoming_result)
        if not service_incoming_result then
            return nil, err
        end

        if (subscriber_incoming_result > subscriber_max + self.burst)
                or (service_incoming_result > service_max + self.burst) then
            local rewind_subscriber_conn, err1 = dict:incr(subscriber_limit_key, -1)
            local rewind_service_conn, err2 = dict:incr(service_limit_key, -1)
            if not rewind_subscriber_conn then
                return nil, err1
            end
            if not rewind_service_conn then
                return nil, err2
            end
            return nil, "rejected"
        end

        self.committed = true
    else
        conn = (dict:get(subscriber_limit_key) or 0) + 1
        if conn > subscriber_max + self.burst then
            return nil, "rejected"
        end
    end

    --if conn > max then
    --    -- make the excessive connections wait
    --    return self.unit_delay * floor((conn - 1) / max), conn
    --end

    -- we return a 0 delay by default
    return 0, subscriber_incoming_result
end


function _M.is_committed(self)
    return self.committed
end


function _M.leaving(self, key, req_latency)
    assert(key)
    local dict = self.dict

    local conn, err = dict:incr(key, -1)
    core.log.notice("xgz【normal】【key = ", key, "】leaving: current conn=", conn)
    if not conn then
        return nil, err
    end

    if req_latency then
        local unit_delay = self.unit_delay
        self.unit_delay = (req_latency + unit_delay) / 2
    end

    return conn
end


function _M.leaving_with_subscriber(self, subscriber_limit_key, service_limit_key, req_latency)
    local finished, conn, err
    finished, conn, err = handle_limit_conn(_M.leaving, self, subscriber_limit_key, service_limit_key, req_latency)
    if finished then
        return conn, err
    end

    local dict = self.dict

    conn, err = dict:incr(subscriber_limit_key, -1)
    core.log.notice("xgz【normal】【key = ", subscriber_limit_key, "】leaving: current conn=", conn)
    if not conn then
        return nil, err
    end

    conn, err = dict:incr(service_limit_key, -1)
    core.log.notice("xgz【normal】【key = ", service_limit_key, "】leaving: current conn=", conn)
    if not conn then
        return nil, err
    end

    if req_latency then
        local unit_delay = self.unit_delay
        self.unit_delay = (req_latency + unit_delay) / 2
    end

    return conn
end


function _M.uncommit(self, key)
    assert(key)
    local dict = self.dict

    return dict:incr(key, -1)
end


function _M.set_conn(self, conn)
    self.max = conn
end


function _M.set_burst(self, burst)
    self.burst = burst
end


return _M