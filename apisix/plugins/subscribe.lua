---
--- @description: 订阅者校验插件
--- @author: xgz
--- @date: 2023/8/15
---

local core             = require("apisix.core")
local common_util      = require("apisix.plugins.common.common-util")
local intercept_result = common_util.intercept_result
local perf_log         = common_util.perf_log
local ngx              = ngx
local ngx_time         = ngx.time
local ngx_now          = ngx.now
local ngx_update_time  = ngx.update_time
local tonumber         = tonumber


local lrucache = core.lrucache.new({
    ttl = 300, count = 512
})


local plugin_name = "subscribe"

local schema = {
    type = "object",
    properties = {
        apps = { description = "apps", type = "object" },
        owner = { description = "owner", type = "string" }
    },
    required = { "apps", "owner" }
}


local _M = {
    version = 0.1,
    priority = 2699,
    type = 'auth',
    name = plugin_name,
    schema = schema
}


local function is_expired(deadline_string)
    --[[
    -- 截止日期格式："YYYY-MM-DD"
    local year, month, day = deadline_string:match("(%d+)-(%d+)-(%d+)")

    year = tonumber(year)
    month = tonumber(month)
    day = tonumber(day)
    local deadline_timestamp = os.time({year = year, month = month, day = day, hour = 23, min = 59, sec = 59})
    --]]

    ngx_update_time()
    local current_timestamp = ngx_time()

    if current_timestamp > tonumber(deadline_string) then
        return true
    else
        return false
    end
end


function _M.check_schema(conf)
    return core.schema.check(schema, conf)
end


function _M.rewrite(conf, ctx)
    local start_time = ngx_now()

    local remote_addr = ctx.var.remote_addr
    core.log.warn("xgz【normal】remote_addr =>" .. remote_addr)

    local headers = core.request.headers(ctx)
    local auth_app_id = common_util.get_header_value("paasid", headers)
    if not auth_app_id then
        return intercept_result(ctx, { errmsg = "此接口需要登录后才能访问", errcode = "ACG.1401" })
    end
    ctx.subscriber_id = auth_app_id

    -- 服务所有者场景：如果订阅者就是服务的所有者，则不用校验IP白名单、订阅者截止日期
    local owner = conf.owner;
    if owner == auth_app_id then
        ctx['sign'] = "1"
        core.log.warn("xgz【normal】self-call")
        return
    end

    local app = conf.apps[auth_app_id];
    if app == nil then
        return intercept_result(ctx, { errmsg = "您没有权限调用此接口", errcode = "AGW.1403" })
    end

    -- IP白名单校验
    local block = false
    if app.whitelist then
        local matcher = lrucache(app.whitelist, nil, core.ip.create_ip_matcher, app.whitelist)
        if matcher then
            block = not matcher:match(remote_addr)
        end
    end

    if block then
        return intercept_result(ctx, { errmsg = "当前IP没有权限调用此接口", errcode = "AGW.1405" })
    end

    -- 订阅者截止日期校验
    local deadline_string = app.deadline
    if deadline_string and deadline_string ~= '' then
        if is_expired(deadline_string) then
            return intercept_result(ctx, { errmsg = "您没有权限调用此接口, 订阅已过期", errcode = "AGW.1407" })
        end
    end

    ctx['sign'] = app.isSign

    perf_log("subscribe", ngx_now() - start_time)
end

return _M
