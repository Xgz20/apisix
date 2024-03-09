---
--- @description: 自定义调用日志存到Elasticsearch中
--- @author: xgz
--- @date: 2023/8/10
---

local core              = require("apisix.core")
local http              = require("resty.http")
local expr              = require("resty.expr.v1")
local service_fetch     = require("apisix.http.service").get
local log_util          = require("apisix.utils.log-util")
local bp_manager_mod    = require("apisix.utils.batch-processor-manager")
local common_util       = require("apisix.plugins.common.common-util")
local get_subscriber_id = common_util.get_subscriber_id
local perf_log          = common_util.perf_log
local ngx               = ngx
local ngx_now           = ngx.now
local req_get_body_data = ngx.req.get_body_data
local str_format        = core.string.format
local math_random       = math.random

local plugin_name = "custom-es-logger"
local batch_processor_manager = bp_manager_mod.new(plugin_name)


local schema = {
    type = "object",
    properties = {
        -- deprecated, use "endpoint_addrs" instead
        endpoint_addr = {
            type = "string",
            pattern = "[^/]$",
        },
        endpoint_addrs = {
            type = "array",
            minItems = 1,
            items = {
                type = "string",
                pattern = "[^/]$",
            },
        },
        field = {
            type = "object",
            properties = {
                index = { type = "string"},
                type = { type = "string"}
            },
            required = {"index"}
        },
        log_format = {type = "object"},
        include_resp_body = {type = "boolean", default = false},
        include_resp_body_expr = {
            type = "array",
            minItems = 1,
            items = {
                type = "array"
            }
        },
        auth = {
            type = "object",
            properties = {
                username = {
                    type = "string",
                    minLength = 1
                },
                password = {
                    type = "string",
                    minLength = 1
                },
            },
            required = {"username", "password"},
        },
        timeout = {
            type = "integer",
            minimum = 1,
            default = 10
        },
        ssl_verify = {
            type = "boolean",
            default = true
        }
    },
    encrypt_fields = {"auth.password"},
    oneOf = {
        {required = {"endpoint_addr", "field"}},
        {required = {"endpoint_addrs", "field"}}
    },
}


local metadata_schema = {
    type = "object",
    properties = {
        log_format = log_util.metadata_schema_log_format,
    },
}


local _M = {
    version = 0.1,
    priority = 414,
    name = plugin_name,
    schema = batch_processor_manager:wrap_schema(schema),
    metadata_schema = metadata_schema,
}


function _M.check_schema(conf, schema_type)
    if schema_type == core.schema.TYPE_METADATA then
        return core.schema.check(metadata_schema, conf)
    end
    return core.schema.check(schema, conf)
end


local function get_normal_log(conf)
    local ctx = ngx.ctx.api_ctx
    local var = ctx.var
    local service_id
    local route_id
    local matched_route = ctx.matched_route and ctx.matched_route.value

    if matched_route then
        service_id = matched_route.service_id or ""
        route_id = matched_route.id
    else
        service_id = var.host
    end

    local consumer
    if ctx.consumer then
        consumer = {
            username = ctx.consumer.username
        }
    end

    local latency, upstream_latency, apisix_latency = log_util.latency_details_in_ms(ctx)

    local log =  {
        response = {
            status = ngx.status
        },
        upstream = var.upstream_addr,
        service_id = service_id,
        route_id = route_id,
        consumer = consumer,
        client_ip = core.request.get_remote_client_ip(ngx.ctx.api_ctx),
        start_time = ngx.req.start_time() * 1000,
        latency = latency,
        upstream_latency = upstream_latency,
        apisix_latency = apisix_latency
    }

    if ctx.resp_body then
        log.response.body = ctx.resp_body
    end

    if conf.include_req_body then

        local log_request_body = true

        if conf.include_req_body_expr then

            if not conf.request_expr then
                local request_expr, err = expr.new(conf.include_req_body_expr)
                if not request_expr then
                    core.log.error('generate request expr err ' .. err)
                    return log
                end
                conf.request_expr = request_expr
            end

            local result = conf.request_expr:eval(ctx.var)

            if not result then
                log_request_body = false
            end
        end

        if log_request_body then
            local body = req_get_body_data()
            if body then
                log.request.body = body
            else
                local body_file = ngx.req.get_body_file()
                if body_file then
                    log.request.body_file = body_file
                end
            end
        end
    end

    return log
end


local function get_logger_entry(conf, ctx)
    local start_time = ngx_now()

    local entry = get_normal_log(conf)

    -- 路由信息
    local matched_route = ctx.matched_route and ctx.matched_route.value

    local route_name
    if matched_route then
        route_name = matched_route.name
    else
        route_name = "UNKNOWN"
    end

    -- 服务信息
    local service_name
    if entry.service_id then
        local service = service_fetch(entry.service_id)
        if not service then
            core.log.error("failed to fetch service configuration by ", "id: ", entry.service_id)
        else
            service_name = service.value.name
        end
    end

    -- 消费者信息
    local consumer_id = get_subscriber_id(ctx)
    local consumer_name

    -- 适配请求头中没传passid场景
    if not consumer_id then
        consumer_id = "UNKNOWN"
        consumer_name = "UNKNOWN"
    end

    if entry.consumer then
        consumer_name = entry.consumer.username
    else
        -- 适配流程还没走到获取消费者信息那一步就被拦截返回的场景（例如，subscribe插件中被白名单拦截）
        consumer_name = consumer_id
    end

    -- 后端服务信息
    local backend_addr
    if ctx.balancer_ip then
        backend_addr = ctx.balancer_ip .. ":" .. ctx.balancer_port
    else
        backend_addr = "UNKNOWN"
    end

    local date_string = os.date("%Y-%m-%d %H:%M:%S", entry.start_time / 1000)
    local date_table = {}
    date_table.year, date_table.month, date_table.day, date_table.hour, date_table.min,
                        date_table.sec = date_string:match("(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)")
    local hour = date_table.hour

    local call_result
    local error_info
    if entry.response.status == 200 then
        -- 适配被拦截时(IP黑名单、白名单等)，按照里约网关规格返回响应码200场景
        if ctx.rio_rsp_body then
            call_result = 0
            error_info = core.json.encode(ctx.rio_rsp_body, true)
        else
            call_result = 1
            error_info = "NA"
        end
    else
        call_result = 0
        if entry.response.body then
            error_info = core.json.encode(entry.response.body, true)
        else
            error_info = "NA"
        end
    end

    local call_log = {
        route_id = entry.route_id,
        route_name = route_name,
        service_id = entry.service_id,
        service_name = service_name,
        consumer_id = consumer_id,
        consumer_name = consumer_name,
        backend_addr = backend_addr,
        call_time = entry.start_time,
        latency = entry.latency,
        upstream_latency = entry.upstream_latency,
        apisix_latency = entry.apisix_latency,
        client_ip = entry.client_ip,
        call_result = call_result,
        error_info = error_info,
        hour = hour
    }

    local result = core.json.encode({
            create = {
                _index = conf.field.index,
                _type = conf.field.type
            }
        }) .. "\n" ..
        core.json.encode(call_log) .. "\n"

    perf_log("custom-es-logger-get_logger_entry", ngx_now() - start_time)

    return result
end


local function send_to_elasticsearch(conf, entries)
    local start_time = ngx_now()

    local httpc, err = http.new()
    if not httpc then
        return false, str_format("create http error: %s", err)
    end

    local selected_endpoint_addr
    if conf.endpoint_addr then
        selected_endpoint_addr = conf.endpoint_addr
    else
        selected_endpoint_addr = conf.endpoint_addrs[math_random(#conf.endpoint_addrs)]
    end
    local uri = selected_endpoint_addr .. "/_bulk"
    local body = core.table.concat(entries, "")
    local headers = {["Content-Type"] = "application/x-ndjson"}
    if conf.auth then
        local authorization = "Basic " .. ngx.encode_base64(
            conf.auth.username .. ":" .. conf.auth.password
        )
        headers["Authorization"] = authorization
    end

    httpc:set_timeout(conf.timeout * 1000)
    local resp, err = httpc:request_uri(uri, {
        ssl_verify = conf.ssl_verify,
        method = "POST",
        headers = headers,
        body = body
    })
    if not resp then
        return false, err
    end

    if resp.status ~= 200 then
        return false, str_format("elasticsearch server returned status: %d, body: %s",
        resp.status, resp.body or "")
    end
    perf_log("custom-es-logger-send_to_elasticsearch", ngx_now() - start_time)

    return true
end

function _M.body_filter(conf, ctx)
    log_util.collect_body(conf, ctx)
end

function _M.log(conf, ctx)
    local entry = get_logger_entry(conf, ctx)
    if batch_processor_manager:add_entry(conf, entry) then
        return
    end

    local process = function(entries)
        return send_to_elasticsearch(conf, entries)
    end

    batch_processor_manager:add_entry_to_new_processor(conf, entry, ctx, process)
end


return _M
