---
--- @description: 判断接口能否跨网络区域访问
--- @author: yfx
--- @date: 2024/01/16
---

local core = require("apisix.core")
local common_util = require("apisix.plugins.common.common-util")
local intercept_result = common_util.intercept_result

local plugin_name = "cross-network"
local schema = {
    type = "object",
    properties = {
        allow = { type = "boolean",
                  default = false,
                  description = "是否允许跨网"
        }
    },
    required = { "allow" }
}

local _M = {
    version = 0.1,
    priority = 2960,
    name = plugin_name,
    schema = schema,
}

function _M.check_schema(conf)
    return core.schema.check(schema, conf)
end

function _M.rewrite(conf, ctx)
    --获取配置里面的allow
    local allow = conf.allow
    if not allow then
        return intercept_result(ctx, { errcode = "AGW.1403", errmsg = "此接口不允许跨网访问" })
    end
end

return _M
