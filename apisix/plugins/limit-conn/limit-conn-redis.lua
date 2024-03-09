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
    _VERSION = '0.08'
}


local mt = {
    __index = _M
}


function _M.new(plugin_name, max, burst, default_conn_delay, conf)
    assert(max > 0 and burst >= 0 and default_conn_delay > 0)

    local self = {
        plugin_name = plugin_name,
        max = max + 0,    -- just to ensure the param is good
        burst = burst,
        unit_delay = default_conn_delay,
        conf = conf,
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
        return red, err
    end

    local max = self.max
    key = _gen_key(self.plugin_name, key)

    self.committed = false

    local conn, err
    if commit then
        conn, err = red:incr(key)
        core.log.warn("xgz【normal】incoming: max=", max, ", burst=", self.burst, ", current conn=", conn)
        if not conn then
            return nil, err
        end

        if conn > max + self.burst then
            conn, err = red:eval(incrby_script, 1, key, -1)
            if not conn then
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
    ---[[
    --core.log.warn("xgz【normal】self.conf.redis_policy=>", self.conf.redis_policy)
    if self.conf.redis_policy == "redis" then
        local ok, err = red:set_keepalive(10000, 100)
        if not ok then
            return nil, err
        end
    end
    --]]

    if conn > max then
        -- make the excessive connections wait
        return self.unit_delay * floor((conn - 1) / max), conn
    end

    -- we return a 0 delay by default
    return 0, conn
end


function _M.is_committed(self)
    return self.committed
end


function _M.leaving(self, key, req_latency)
    assert(key)

    local conf = self.conf
    local red, err = redis_cli(conf)
    if not red then
        return red, err
    end

    key = _gen_key(self.plugin_name, key)

    local conn, err = red:eval(incrby_script, 1, key, -1)
    core.log.warn("xgz【normal】leaving: current conn=", conn)
    if not conn then
        return nil, err
    end

    if req_latency then
        local unit_delay = self.unit_delay
        self.unit_delay = (req_latency + unit_delay) / 2
    end

    if self.conf.redis_policy == "redis" then
        local ok, err = red:set_keepalive(10000, 100)
        if not ok then
            return nil, err
        end
    end

    return conn
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