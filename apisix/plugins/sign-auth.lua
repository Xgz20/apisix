---
--- @description: 一次认证插件
--- @author: xgz
--- @date: 2023/8/15
---

local core             = require("apisix.core")
local consumer_mod     = require("apisix.consumer")
local common_util      = require("apisix.plugins.common.common-util")
local get_header_value = common_util.get_header_value
local intercept_result = common_util.intercept_result
local perf_log         = common_util.perf_log
local resty_sha256     = require("resty.sha256")
local restystr         = require("resty.string")
local ngx              = ngx
local ngx_now          = ngx.now
local math             = math
local string_upper     = string.upper
local string_format    = string.format


local plugin_name = "sign-auth"


local schema = {
    type = "object",
    properties = {
        header = {
            type = "string",
            default = "apikey",
        },
        query = {
            type = "string",
            default = "apikey",
        },
        hide_credentials = {
            type = "boolean",
            default = false,
        }
    },
}


local consumer_schema = {
    type = "object",
    properties = {
        key = {type = "string"},
        app_secret = {type = "string"}
    },
    encrypt_fields = {"app_secret"},
    required = {"key", "app_secret"},
}


local _M = {
    version = 0.1,
    priority = 2698,
    type = 'auth',
    name = plugin_name,
    schema = schema,
    consumer_schema = consumer_schema
}


local function get_signature(auth_app_secret, auth_nonce, auth_timestamp)
    local values = { auth_timestamp, auth_app_secret, auth_nonce, auth_timestamp }
    local string_to_sign = ''
    for i = 0, #values do
        if values[i] ~= nil and values[i] ~= '' then
            string_to_sign = string_to_sign .. values[i]
        end
    end

    local sha256 = resty_sha256:new()
    sha256:update(string_to_sign)
    local digest = sha256:final()
    local signature = string_upper(restystr.to_hex(digest))
    return signature, string_to_sign
end


function _M.check_schema(conf)
    local ok, err
    if schema_type == core.schema.TYPE_CONSUMER then
        ok, err = core.schema.check(consumer_schema, conf)
    else
        return core.schema.check(schema, conf)
    end

    if not ok then
        return false, err
    end
end


function _M.rewrite(conf, ctx)
    --core.log.warn("xgz【ctx】ctx.headers =>", core.json.encode(ctx.headers, true))
    local start_time = ngx_now()

    -- 获取消费者
    local from_header = true
    local headers = ctx.headers
    if not headers then
        headers = core.request.headers(ctx)
    end

    local key = ctx.subscriber_id
    if not key then
        key = get_header_value("paasid", headers)
    end
    if not key then
        local uri_args = core.request.get_uri_args(ctx) or {}
        key = uri_args[conf.query]
        from_header = false
    end

    if not key then
        return intercept_result(ctx, { errmsg = "此接口需要登录后才能访问", errcode = "ACG.1401" })
    end
    core.log.warn("xgz【normal】subscriber key =>" .. key)

    local consumer_conf = consumer_mod.plugin(plugin_name)
    if not consumer_conf then
        return 401, {message = "Missing related consumer"}
    end

    local consumers = consumer_mod.consumers_kv(plugin_name, consumer_conf, "key")
    local consumer = consumers[key]
    if not consumer then
        return 401, {message = "Invalid API key in request"}
    end
    --core.log.warn("xgz【normal】consumer =>", core.json.delay_encode(consumer))
    core.log.warn("xgz【normal】consumer_name =>", consumer.consumer_name)

    if conf.hide_credentials then
        if from_header then
            core.request.set_header(ctx, conf.header, nil)
        else
            local args = core.request.get_uri_args(ctx)
            args[conf.query] = nil
            core.request.set_uri_args(ctx, args)
        end
    end

    consumer_mod.attach_consumer(ctx, consumer, consumer_conf)

    core.log.info("hit sign-auth rewrite")
    perf_log("attach_consumer", ngx_now() - start_time)

    -- 一次签名认证的逻辑

    local sign = ctx['sign']
    if sign ~= '1' then
        return
    end

    -- 签名相关参数从header获取
    local auth_timestamp = get_header_value("timestamp", headers)
    local auth_signature = get_header_value("signature", headers)
    local auth_nonce = get_header_value("nonce", headers)
    local result = { errmsg = "请求签名错误或请求服务器时间戳误差大于 180 秒", errcode = "AGW.1433" }
    if not (auth_timestamp and auth_signature) then
        return intercept_result(ctx, result)
    end

    -- 签名时间戳超时校验（防重放攻击）
    local diff = math.abs(ngx.time() - auth_timestamp)
    if diff > 60 * 3 then
        return intercept_result(ctx, result)
    end

    -- 从消费者中获取应用秘钥
    local auth_app_secret = consumer.auth_conf.app_secret
    if not auth_app_secret or auth_app_secret == '' then
        return intercept_result(ctx, result)
    end

    -- 签名校验逻辑
    local signature, string_to_sign = get_signature(auth_app_secret, auth_nonce, auth_timestamp);

    auth_signature = string_upper(auth_signature)
    if auth_signature ~= signature then
        core.log.error(string_format("stringToSign:%s , param authSignature:%s, signature:%s",
                string_to_sign, auth_signature, signature))
        return intercept_result(ctx, result)
    end

    perf_log("sign-auth", ngx_now() - start_time)
end

return _M
