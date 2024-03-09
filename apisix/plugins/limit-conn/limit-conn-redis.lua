---
--- @description: 集群模式下，使用Redis实现并发数流控算法
--- @author: xgz
--- @date: 2023/11/15
---

local core         = require("apisix.core")
local redis_cli    = require("apisix.plugins.common.common-util").redis_cli
local math         = require "math"
local floor        = math.floor
local setmetatable = setmetatable
local assert       = assert
local tostring     = tostring


local _M = {
    _VERSION = '1.0.0'
}


local mt = {
    __index = _M
}


function _M.new(plugin_name, subscriber_conn, service_conn, burst, default_conn_delay, conf)
    assert(burst >= 0 and default_conn_delay > 0)

    local self = {
        plugin_name = plugin_name,
        subscriber_max = subscriber_conn,
        service_max = service_conn,
        burst = burst,
        unit_delay = default_conn_delay,
        conf = conf,
        max = conf.conn
    }

    return setmetatable(self, mt)
end


local function _gen_key(plugin_name, key)
    return plugin_name .. "#" .. tostring(key)
end

local incrby_script = core.string.compress_script([=[
    return redis.call('incrby', KEYS[1], ARGV[1])
]=])

function _M.incoming(self, key, commit)
    local conf = self.conf
    local red, err = redis_cli(conf)
    if not red then
        core.log.error("incoming create redis_cli error: ", err)
        return red, err
    end

    local max = self.max
    key = _gen_key(self.plugin_name, key)

    self.committed = false

    local conn, err
    if commit then
        conn, err = red:incr(key)
        if not conn then
            core.log.error("incoming incr error: ", err)
            return nil, err
        end

        core.log.notice("xgz【normal】【key = ", key, "】incoming: max=", max,
                ", burst=", self.burst, ", current conn=", conn)

        if conn > max + self.burst then
            conn, err = red:eval(incrby_script, 1, key, -1)
            if not conn then
                core.log.error("incoming eval error: ", err)
                return nil, err
            end
            return nil, "rejected"
        end
        self.committed = true

    else
        conn = (red:get(key) or 0) + 1
        if conn > max + self.burst then
            return nil, "rejected"
        end
    end

    -- 10秒的空闲时间，最多保持 100 个连接在连接池中
    --core.log.notice("xgz【normal】self.conf.redis_policy =>", self.conf.redis_policy)
    if self.conf.redis_policy == "redis" then
        local ok, err = red:set_keepalive(10000, 100)
        if not ok then
            return nil, err
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

    local subscriber_max = self.subscriber_max
    local service_max = self.service_max

    local conf = self.conf
    local red, err = redis_cli(conf)
    if not red then
        core.log.error("incoming_with_subscriber create redis_cli error: ", err)
        return red, err
    end

    subscriber_limit_key = _gen_key(self.plugin_name, subscriber_limit_key)
    service_limit_key = _gen_key(self.plugin_name, service_limit_key)

    self.committed = false
    local subscriber_incoming_result
    local service_incoming_result
    if commit then
        red:init_pipeline()

        red:incr(subscriber_limit_key)
        red:incr(service_limit_key)

        local res, err = red:commit_pipeline()
        if not res then
            core.log.error("incoming incr pipeline err: ", err)
            return nil, err
        end
        --core.log.notice("incoming result: ", core.json.encode(res))

        subscriber_incoming_result = res[1]
        service_incoming_result = res[2]

        if type(subscriber_incoming_result) == "table" then
            return nil, subscriber_incoming_result[2]
        end

        if type(service_incoming_result) == "table" then
            return nil, service_incoming_result[2]
        end

        core.log.notice("xgz【normal】【key = ", subscriber_limit_key, "】incoming: max=", subscriber_max,
                ", burst=", self.burst, ", current conn=", subscriber_incoming_result)
        core.log.notice("xgz【normal】【key = ", service_limit_key, "】incoming: max=", service_max,
                ", burst=", self.burst, ", current conn=", service_incoming_result)

        if (subscriber_incoming_result > subscriber_max + self.burst)
                or (service_incoming_result > service_max + self.burst) then
            red:init_pipeline()

            red:eval(incrby_script, 1, subscriber_limit_key, -1)
            red:eval(incrby_script, 1, service_limit_key, -1)

            local res, err = red:commit_pipeline()
            if not res then
                core.log.error("incoming eval pipeline err: ", err)
                return nil, err
            end

            return nil, "rejected"
        end

        self.committed = true
    else
        conn = (red:get(subscriber_limit_key) or 0) + 1
        if conn > subscriber_max + self.burst then
            return nil, "rejected"
        end
    end

    -- 10秒的空闲时间，最多保持 100 个连接在连接池中
    --core.log.notice("xgz【normal】self.conf.redis_policy=>", self.conf.redis_policy)
    if self.conf.redis_policy == "redis" then
        local ok, err = red:set_keepalive(10000, 100)
        if not ok then
            return nil, err
        end
    end

    --if conn > subscriber_max then
    --    core.log.alert("正常应该不会走到这个流程！！！")
    --    -- make the excessive connections wait
    --    return self.unit_delay * floor((conn - 1) / subscriber_max), conn
    --end

    -- we return a 0 delay by default
    return 0, subscriber_incoming_result
end


function _M.is_committed(self)
    return self.committed
end


function _M.leaving(self, key, req_latency)
    assert(key)

    local conf = self.conf
    local red, err = redis_cli(conf)
    if not red then
        core.log.error("leaving create redis_cli error: ", err)
        return red, err
    end

    key = _gen_key(self.plugin_name, key)

    local conn, err = red:eval(incrby_script, 1, key, -1)
    core.log.notice("xgz【normal】【key = ", key, "】leaving: current conn=", conn)
    if not conn then
        core.log.error("leaving redis eval error: ", err)
        return nil, err
    end

    if req_latency then
        local unit_delay = self.unit_delay
        self.unit_delay = (req_latency + unit_delay) / 2
    end

    if self.conf.redis_policy == "redis" then
        local ok, err = red:set_keepalive(10000, 100)
        if not ok then
            core.log.error("leaving redis set_keepalive error: ", err)
            return nil, err
        end
    end

    return conn
end


function _M.leaving_with_subscriber(self, subscriber_limit_key, service_limit_key, req_latency)
    local finished, conn, err = handle_limit_conn(_M.leaving, self, subscriber_limit_key, service_limit_key, req_latency)
    if finished then
        return conn, err
    end

    local conf = self.conf
    local red, err = redis_cli(conf)
    if not red then
        core.log.error("leaving_with_subscriber create redis_cli error: ", err)
        return red, err
    end

    subscriber_limit_key = _gen_key(self.plugin_name, subscriber_limit_key)
    service_limit_key = _gen_key(self.plugin_name, service_limit_key)

    red:init_pipeline()

    red:eval(incrby_script, 1, subscriber_limit_key, -1)
    red:eval(incrby_script, 1, service_limit_key, -1)

    local res, err = red:commit_pipeline()
    if not res then
        core.log.error("leaving redis pipeline error: ", err)
        return nil, err
    end
    --core.log.notice("leaving result: ", core.json.encode(res))

    local subscriber_limit_result = res[1]
    local service_limit_result = res[2]
    if type(subscriber_limit_result) == "table" then
        core.log.error("subscriber limit【key = ", subscriber_limit_key,
                "】leaving err: ", subscriber_limit_result[2])
        return nil, subscriber_limit_result[2]
    end

    if type(service_limit_result) == "table" then
        core.log.error("subscriber limit【key = ", service_limit_key,
                "】leaving err: ", service_limit_result[2])
        return nil, service_limit_result[2]
    end

    core.log.notice("xgz【normal】【key = ", subscriber_limit_key,
            "】leaving: current conn=", subscriber_limit_result)
    core.log.notice("xgz【normal】【key = ", service_limit_key,
            "】leaving: current conn=", service_limit_result)

    if req_latency then
        local unit_delay = self.unit_delay
        self.unit_delay = (req_latency + unit_delay) / 2
    end

    if self.conf.redis_policy == "redis" then
        local ok, err = red:set_keepalive(10000, 100)
        if not ok then
            core.log.error("leaving_with_subscriber redis set_keepalive error: ", err)
            return nil, err
        end
    end

    return subscriber_limit_result
end


function _M.uncommit(self, key)
    assert(key)
    local conf = self.conf
    local red, err = redis_cli(conf)
    if not red then
        return red, err
    end

    return red:eval(incrby_script, 1, key, -1)
end


function _M.set_conn(self, conn)
    self.max = conn
end


function _M.set_burst(self, burst)
    self.burst = burst
end


return _M
