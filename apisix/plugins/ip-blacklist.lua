---
--- @description: IP黑名单（支持启用、禁用；生效时间段，另外支持一个服务绑定多个黑名单）
--- @author: xgz
--- @date: 2023/8/14
---

local core             = require("apisix.core")
local common_util      = require("apisix.plugins.common.common-util")
local intercept_result = common_util.intercept_result
local perf_log         = common_util.perf_log
local ipairs           = ipairs
local ngx              = ngx
local ngx_time         = ngx.time
local ngx_now          = ngx.now
local ngx_update_time  = ngx.update_time

local lrucache  = core.lrucache.new({
    ttl = 300, count = 512
})


local plugin_name = "ip-blacklist"

local schema = {
    type = "object",
    properties = {
        all_blacklist = {
            description = "all_blacklist",
            type = "object",
            patternProperties = {
                [".*"] = {
                    description = "the single ip blacklist",
                    type = "object",
                    properties = {
                        enable = {
                            type = "boolean",
                            default = false
                        },
                        effective_time = {
                            type = "object",
                            properties = {
                                type = {
                                    type = "string",
                                    enum = {"custom", "forever"},
                                    default = "custom"
                                }
                            },
                            dependencies = {
                                type = {
                                    oneOf = {
                                        {
                                            properties = {
                                                type = {
                                                    enum = {"custom"},
                                                },
                                                start_time = {
                                                    type = "string",
                                                },
                                                end_time = {
                                                    type = "string",
                                                }
                                            },
                                            required = {"start_time", "end_time"},
                                        },
                                        {
                                            properties = {
                                                type = {
                                                    enum = {"forever"},
                                                },
                                            },
                                        }
                                    }
                                }
                            },
                        },
                        message = {
                            type = "object",
                            default = { errmsg = "当前IP没有权限调用此接口", errcode = "AGW.1405" }
                        },
                        blacklist = {
                            type = "array",
                            items = {anyOf = core.schema.ip_def},
                            minItems = 1
                        },
                    },
                    required = { "blacklist", "effective_time" }
                }
            }
        }
    },
}

local _M = {
    version = 0.1,
    priority = 3001,
    name = plugin_name,
    schema = schema,
}


function _M.check_schema(conf)
    local ok, err = core.schema.check(schema, conf)

    if not ok then
        return false, err
    end

    -- we still need this as it is too complex to filter out all invalid IPv6 via regex
    if conf.blacklist then
        for _, cidr in ipairs(conf.blacklist) do
            if not core.ip.validate_cidr_or_ip(cidr) then
                return false, "invalid ip address: " .. cidr
            end
        end
    end

    return true
end


local function date_to_timestamp(date_time)
    local date_table = {}
    date_table.year, date_table.month, date_table.day, date_table.hour, date_table.min,
                date_table.sec = date_time:match("(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)")
    local timestamp = os.time(date_table)
    return timestamp
end


local function is_expired(conf)
    local start_timestamp = date_to_timestamp(conf.effective_time.start_time)
    local end_timestamp = date_to_timestamp(conf.effective_time.end_time)

    ngx_update_time()
    local current_timestamp = ngx_time()

    if current_timestamp >= start_timestamp and current_timestamp <= end_timestamp then
        return false
    else
        return true
    end
end


function _M.rewrite(conf, ctx)
    local start_time = ngx_now()

    local all_blacklist = conf.all_blacklist

    if not all_blacklist or type(all_blacklist) ~= "table" or core.table.isempty(all_blacklist) then
        return
    end

    local remote_addr = ctx.var.remote_addr

    for blacklist_id, blacklist_info in pairs(all_blacklist) do
        if not blacklist_info.enable then
            goto CONTINUE
        end

        if blacklist_info.effective_time.type == "custom" and is_expired(blacklist_info) then
            goto CONTINUE
        end

        local block = false

        if blacklist_info.blacklist then
            local matcher = lrucache(blacklist_info.blacklist, nil,
                    core.ip.create_ip_matcher, blacklist_info.blacklist)
            if matcher then
                block = matcher:match(remote_addr)
            end
        end

        if block then
            core.log.warn("触发IP黑名单，ID：【" .. blacklist_id .. "】")
            return intercept_result(ctx, blacklist_info.message)
        end

        ::CONTINUE::
    end

    perf_log("ip-blacklist", ngx_now() - start_time)
end


return _M
