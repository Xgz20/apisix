---
--- @description: uri白名单（实现跨网络区域路由访问）
--- @author: xgz
--- @date: 2023/11/10
---

local core             = require("apisix.core")
local re_compile       = require("resty.core.regex").re_match_compile
local common_util      = require("apisix.plugins.common.common-util")
local intercept_result = common_util.intercept_result
local re_find          = ngx.re.find
local ipairs           = ipairs

local schema = {
    type = "object",
    properties = {
        allowed_uris = {
            type = "array",
            items = {
                type = "string",
                minLength = 1,
                maxLength = 4096,
            },
            uniqueItems = true
        },
    },
    required = {"allowed_uris"},
}


local plugin_name = "uri-whitelist"

local _M = {
    version = 0.1,
    priority = 2970,
    name = plugin_name,
    schema = schema,
    type = 'auth'
}


function _M.check_schema(conf)
    local ok, err = core.schema.check(schema, conf)
    if not ok then
        return false, err
    end

    for _, re_rule in ipairs(conf.allowed_uris) do
        local ok, err = re_compile(re_rule, "j")
        -- core.log.warn("ok: ", tostring(ok), " err: ", tostring(err),
        --               " re_rule: ", re_rule)
        if not ok then
            return false, err
        end
    end

    return true
end


function _M.rewrite(conf, ctx)
    local request_uri = ctx.var.request_uri
    core.log.notice("uri: ", request_uri)
    if request_uri:sub(-1) == "/" then
        request_uri = request_uri:sub(1, -2)
    end

    if not conf.normal_allowed_uris or not conf.site_allowed_uris_concat then
        local normal_allowed_uris = {}
        local site_allowed_uris = {}
        local j = 0
        for _, re_rule in ipairs(conf.allowed_uris) do
            if re_rule:sub(-1) == "/" then
                j = j + 1
                site_allowed_uris[j] = re_rule
            else
                normal_allowed_uris[re_rule] = true
            end
        end

        conf.normal_allowed_uris = normal_allowed_uris
        conf.site_allowed_uris_concat = core.table.concat(site_allowed_uris, "|")

        --core.log.info("normal_allowed_uris: ", core.json.encode(conf.normal_allowed_uris, true))
        core.log.info("site_allowed_uris_concat: ", conf.site_allowed_uris_concat)
    end

    if not conf.normal_allowed_uris[request_uri]
            and re_find(request_uri .. "/", conf.site_allowed_uris_concat, "jo") ~= 1 then
        return intercept_result(ctx, {errcode = "AGW.1403", errmsg = "此接口不允许跨网访问"})
    end
end


return _M