---
--- @description: 实现各种类型的监控告警(支持单机和集群场景)，并将告警日志推送到ES中
--- @author: xgz
--- @date: 2023/9/5
---

local core                 = require("apisix.core")
local http                 = require("resty.http")
local log_util             = require("apisix.utils.log-util")
local deepcopy             = require("apisix.core.table").deepcopy
local bp_manager_mod       = require("apisix.utils.batch-processor-manager")
local common_util          = require("apisix.plugins.common.common-util")
local perf_log             = common_util.perf_log
local read_redis_conf      = common_util.read_redis_conf
local redis_cli            = common_util.redis_cli
local init_plugin_metadata = common_util.init_plugin_metadata
local ngx                  = ngx
local ngx_now              = ngx.now
local str_format           = core.string.format
local math_random          = math.random
local math_floor           = math.floor

local plugin_name = "monitor-alert"

local batch_processor_manager = bp_manager_mod.new(plugin_name)
local shared_buffer = ngx.shared["plugin-" .. plugin_name]
if not shared_buffer then
    error("failed to get ngx.shared dict when load plugin " .. plugin_name)
end

local cluster_info = {}

local schema = {
    type = "object",
    properties = {
        all_alarm_rule = {
            description = "告警规则列表",
            type = "object",
            patternProperties = {
                [".*"] = {
                    description = "the single alarm rule",
                    type = "object",
                    properties = {
                        alarm_rule_name = {
                            description = "告警规则名称",
                            type = "string"
                        },
                        alarm_type = {
                            description = "告警类型",
                            type = "integer",
                            default = 1,
                            enum = { 1, 2, 3, 4, 5, 6, 7 },
                        },
                        monitor_period = {
                            description = "监控周期（单位为分钟）",
                            type = "integer",
                            minimum = 1,
                            default = 1,
                            --enum = { 1, 5, 15, 30, 60 },
                        },
                        duration_count = {
                            description = "持续周期数目",
                            type = "integer",
                            default = 1,
                            enum = { 1, 3, 5, 10, 15, 30, 60 },
                        },
                        metric_threshold = {
                            description = "指标阈值（可能是次数、毫秒数、KB字节数）",
                            type = "number",
                        },
                        alarm_level = {
                            description = "告警级别",
                            type = "string",
                            default = "紧急",
                            enum = { "紧急", "重要", "次要", "提示" },
                        },
                        enable = {
                            type = "boolean",
                            default = true
                        }
                    },
                    required = { "alarm_rule_name", "metric_threshold" }
                }
            },
        },
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
        },
    },
    encrypt_fields = {"auth.password"},
    oneOf = {
        {required = {"endpoint_addr", "field", "all_alarm_rule"}},
        {required = {"endpoint_addrs", "field", "all_alarm_rule"}}
    }
}


local metadata_schema = {
    type = "object",
    properties = {
        log_format = log_util.metadata_schema_log_format,
    },
}


local _M = {
    version = 0.1,
    priority = 415,
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


local ALARM_RULE_TYPE = {
    { code = 1, name = "接口调用次数", desc = "%s 监控周期%s分钟 连续%s个周期 总计%s次" },
    { code = 2, name = "2XX调用次数", desc = "%s 监控周期%s分钟 连续%s个周期 总计%s次" },
    { code = 3, name = "4XX调用次数", desc = "%s 监控周期%s分钟 连续%s个周期 总计%s次" },
    { code = 4, name = "5XX调用次数", desc = "%s 监控周期%s分钟 连续%s个周期 总计%s次" },
    { code = 5, name = "延时时间", desc = "%s 监控周期%s分钟 连续%s个周期 平均%sms" },
    { code = 6, name = "异常次数", desc = "%s 监控周期%s分钟 连续%s个周期 总计%s次" },
    { code = 7, name = "流入流量", desc = "%s 监控周期%s分钟 连续%s个周期 总计%sKBytes" }
}


local function get_logger_entry(conf, ctx, alarm_rule, log_entry)
    --core.log.warn("xgz【normal】=>", core.json.encode(alarm_rule, true))
    local start_time = ngx_now()

    -- 路由信息
    local matched_route = ctx.matched_route and ctx.matched_route.value
    core.log.warn("xgz【normal】matched_route =>", core.json.encode(matched_route, true))

    local route_name
    if matched_route then
        route_name = matched_route.name
    else
        route_name = "UNKNOWN"
    end

    local alarm_log = {
        -- 告警规则名称
        alarm_rule_name = alarm_rule.alarm_rule_name,
        -- 产生时间（毫秒级的时间戳）
        create_time = log_entry.start_time,
        -- 服务名称
        route_name = route_name,
        -- 告警规则描述
        alarm_rule_desc = str_format(ALARM_RULE_TYPE[alarm_rule.alarm_type].desc, ALARM_RULE_TYPE[alarm_rule.alarm_type].name,
                alarm_rule.monitor_period, alarm_rule.duration_count, alarm_rule.metric_threshold),
        -- 告警级别
        alarm_level = alarm_rule.alarm_level
    }

    core.log.warn("xgz【normal】alarm_log =>", core.json.encode(alarm_log, true))

    local result = core.json.encode({
        create = {
            _index = conf.field.index,
            _type = conf.field.type
        }
        }) .. "\n" .. core.json.encode(alarm_log) .. "\n"

    perf_log("monitor-alert-get_logger_entry", ngx_now() - start_time)

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

    core.log.warn("xgz【normal】=>", "uri: ", uri, ", body: ", body)

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

    perf_log("monitor-alert-send_to_elasticsearch", ngx_now() - start_time)

    return true
end


local function is_Xxx(http_status, basic_code)
    if http_status and http_status >= basic_code and http_status < basic_code + 100 then
        if basic_code == 200 then
            return false
        else
            return true
        end
    end

    return false
end


local function validate_alarm_type_condition(alarm_type, validation_rules)
    local increment_value = 1
    if alarm_type == ALARM_RULE_TYPE[1].code then
        -- "接口调用次数"类型告警不做校验
        return true, increment_value
    elseif alarm_type == ALARM_RULE_TYPE[2].code then
        if is_Xxx(validation_rules.response_status, 200) then
            return true, increment_value
        end
    elseif alarm_type == ALARM_RULE_TYPE[3].code then
        if is_Xxx(validation_rules.response_status, 400) then
            return true, increment_value
        end
    elseif alarm_type == ALARM_RULE_TYPE[4].code then
        if is_Xxx(validation_rules.response_status, 500) then
            return true, increment_value
        end
    elseif alarm_type == ALARM_RULE_TYPE[5].code then
        return true, validation_rules.latency
    elseif alarm_type == ALARM_RULE_TYPE[6].code then
        if validation_rules.response_status ~= 200 then
            return true, increment_value
        end
    elseif alarm_type == ALARM_RULE_TYPE[7].code then
        return true, validation_rules.inbound_traffic
    end

    return false, nil
end


local function init_sampling_info(start_sampling_time, increment_value)
    return {
        start_sampling_time = start_sampling_time, -- 最新采样时间
        current_value = increment_value, -- 指标数据
        current_count = 1, -- 调用次数
        cycle_period = 1 -- 采样周期数目
    }
end


local function calculate_minute_difference(call_time, start_sampling_time, is_debug_mode)
    if is_debug_mode then
        return (call_time - start_sampling_time) / (60 * 1000)
    end

    return math_floor((call_time - start_sampling_time) / (60 * 1000))
end


local function update_sampling_info(running_sampler, increment_value, start_sampling_time, is_new_period)
    if is_new_period then
        running_sampler.current_count = 1
        running_sampler.start_sampling_time = start_sampling_time
        running_sampler.cycle_period = running_sampler.cycle_period + 1
        running_sampler.current_value = increment_value
    else
        running_sampler.current_count = running_sampler.current_count + 1
        running_sampler.current_value = running_sampler.current_value + increment_value
    end
end


local function is_exceeding_threshold(running_sampling_info, alarm_rule)
    local alarm_type = alarm_rule.alarm_type
    local metric_threshold = alarm_rule.metric_threshold

    local total_count = running_sampling_info.current_count
    local total_value = running_sampling_info.current_value

    if alarm_type == ALARM_RULE_TYPE[5].code then
        return (total_value / total_count) >= metric_threshold
    elseif alarm_type == ALARM_RULE_TYPE[7].code then
        return (total_value / 1024) >= metric_threshold
    else
        return total_value >= metric_threshold
    end
end


local function is_exceeding_period(running_sampling_info, alarm_rule)
    return running_sampling_info.cycle_period >= alarm_rule.duration_count
end


local function get_time_str(timestamp)
    return os.date("%Y-%m-%d %H:%M:%S", timestamp / 1000)
end


local set_script = core.string.compress_script([=[
    return redis.call('set', KEYS[1], ARGV[1])
]=])


local get_script = core.string.compress_script([=[
    return redis.call('get', KEYS[1])
]=])


local function set_data_to_redis(conf, key, value)
    core.log.warn("xgz【normal】conf=>", core.json.encode(conf, true))

    local red, err = redis_cli(conf)
    if not red then
        core.log.error("xgz【normal】redis_cli error=>", err)
    end

    local res
    res, err = red:eval(set_script, 1, key, value)

    if err then
        core.log.error("xgz【normal】redis eval set err=>", err)
    end
end


local function get_data_from_redis(conf, key)
    core.log.warn("xgz【normal】conf=>", core.json.encode(conf, true))

    local red, err = redis_cli(conf)
    if not red then
        core.log.error("xgz【normal】redis_cli error=>", err)
        return nil
    end

    local res
    res, err = red:eval(get_script, 1, key)

    if err then
        core.log.error("xgz【normal】redis eval get err=>", err)
        return nil
    else
        core.log.warn("xgz【normal】redis eval get result=>", res)
        return res
    end
end


local function gen_redis_key(key)
    local key_tab = {}
    key_tab[1] = "plugin-"
    key_tab[2] = plugin_name
    key_tab[3] = "-"
    key_tab[4] = key
    return core.table.concat(key_tab, "")
end


local function set_opt(key, value, conf)
    local value_str, err_msg = core.json.encode(value)
    if not value_str then
        core.log.error("encode data got error: ", err_msg, ", input parameter: ", value)
        return
    end

    if conf.policy == "local" then
        local success, err, forcible = shared_buffer:set(key, value_str)
        if success then
            if forcible then
                core.log.error("monitor sampling store is out of memory")
            end
        else
            core.log.error("monitor sampling store error: ", err)
        end
    else
        set_data_to_redis(conf, gen_redis_key(key), value_str)
    end
end


local function get_opt(key, conf)
    local json_str
    if conf.policy == "local" then
        json_str = shared_buffer:get(key)
    else
        json_str = get_data_from_redis(conf, gen_redis_key(key))
    end

    if json_str then
        local result, err = core.json.decode(json_str)
        if err then
            core.log.error("decode data got error: ", err, ", input parameter: ", json_str)
        end
        return result
    else
        return nil
    end
end


local function check_if_alerted(conf, ctx, alarm_rule_id, alarm_rule, validation_metadata)
    local log_warn = function(log_type, ...)
            core.log.warn(str_format("xgz【%s】【%s】", log_type, alarm_rule_id), ...)
        end

    log_warn("normal", "--------------------------------监控告警start-----------------------------------")

    -- 前置条件校验，并获取本次请求增量数据（调用次数、延时 or 流入流量）
    local ok, increment_value = validate_alarm_type_condition(alarm_rule.alarm_type, validation_metadata)
    core.log.warn("xgz【normal】increment_value=>", increment_value)
    if not ok then
        return false
    end

    local call_time = validation_metadata.call_time

    -- 告警采样逻辑
    local sampling_key = ctx.route_id .. "_" .. alarm_rule_id

    local running_sampler = get_opt(sampling_key, conf)
    local initial_sampler = init_sampling_info(validation_metadata.call_time, increment_value)

    log_warn("normal", "alarm_rule_id=>", alarm_rule_id)
    log_warn("normal", "alarm_rule=>", core.json.encode(alarm_rule, true))
    log_warn("normal", "sampling_key=>", sampling_key)
    log_warn("normal", "running_sampler=>", core.json.encode(running_sampler, true))
    log_warn("normal", "initial_sampler=>", core.json.encode(initial_sampler, true))
    log_warn("normal", "call_time=>", call_time, "|", get_time_str(call_time))

    -- 重置采样器
    local reset_sampler = function() set_opt(sampling_key, initial_sampler, conf) end

    -- step1：判断是否已有采样记录
    if running_sampler then
        -- 已有采样记录
        local start_sampling_time = running_sampler.start_sampling_time
        log_warn("normal", "start_sampling_time=>", start_sampling_time, "|", get_time_str(start_sampling_time))
        local pass_minutes = calculate_minute_difference(call_time, start_sampling_time)
        log_warn("normal", "pass_minutes=>",
                calculate_minute_difference(call_time, start_sampling_time, true))

        -- 监控周期（分钟）
        local monitor_period = alarm_rule.monitor_period
        log_warn("normal", "monitor_period=>", monitor_period)

        -- step2：判断是否超过单个监控周期
        if pass_minutes >= monitor_period then
            local next_sampling_time = start_sampling_time + monitor_period * 60 * 1000
            log_warn("normal", "next_sampling_time=>", next_sampling_time, "|", get_time_str(next_sampling_time))
            pass_minutes = calculate_minute_difference(call_time, next_sampling_time)
            log_warn("normal", "next pass_minutes=>",
                    calculate_minute_difference(call_time, next_sampling_time, true))

            log_warn("normal", "------------------------------------监控告警end-------------------------------")

            -- step3：超过了单个监控周期，继续判断是否在下个监控周期内
            if pass_minutes > 0 then
                -- 不在下个监控周期内，清空历史采样数据并重新初始化
                reset_sampler()
                log_warn("alert", "不在下个监控周期内，清空历史采样数据并重新初始化")
                return false
            else
                -- step4：在下个监控周期内，判断是否超过阈值
                if is_exceeding_threshold(running_sampler, alarm_rule) then

                    -- step5：超过了阈值，再判断是否超过持续周期数目
                    if is_exceeding_period(running_sampler, alarm_rule) then
                        -- 超过告警规则设置的持续周期数目，则进行告警，并重新采样计数
                        reset_sampler()
                        log_warn("alert", "超过告警规则设置的持续周期数目，则进行告警，并重新采样计数")
                        return true
                    else
                        -- 未超过告警规则设置的持续周期数目，则用当前值更新采样器，并将持续周期数+1
                        update_sampling_info(running_sampler, increment_value, next_sampling_time, true)
                        set_opt(sampling_key, running_sampler, conf)
                        log_warn("alert", "未超过告警规则设置的持续周期数目，则用当前值更新采样器，并将持续周期数+1")
                        return false
                    end
                else
                    -- 未超过告警阈值，则重置采样器
                    reset_sampler()
                    log_warn("alert", "未超过告警阈值，则重置采样器")
                    return false
                end
            end
        else
            -- 还在单个采样周期内，则更新采样器
            update_sampling_info(running_sampler, increment_value, start_sampling_time, false)
            set_opt(sampling_key, running_sampler, conf)
            log_warn("alert", "还在单个采样周期内，则更新采样器")
            return false
        end
    else
        -- 之前没有采样记录，则初始化采样器
        reset_sampler()
        log_warn("alert", "之前没有采样记录，则初始化采样器")
        return false
    end
end


function _M.body_filter(conf, ctx)
    log_util.collect_body(conf, ctx)
end


local function async_check(premature, conf, ctx , validation_metadata, log_entry)
    --core.log.warn("xgz【conf】async_check=>", core.json.encode(conf, true))
    --core.log.warn("xgz【ctx】async_check=>", core.json.encode(ctx, true))
    local start_time = ngx_now()

    if premature then
        return
    end
    -- 是否告警
    for alarm_rule_id, alarm_rule in pairs(conf.all_alarm_rule) do
        if not alarm_rule.enable then
            core.log.warn(str_format("告警规则【%s】disabled", alarm_rule_id))
            goto CONTINUE
        end

        -- 告警检查逻辑
        local start_time2 = ngx_now()
        local alert = check_if_alerted(conf, ctx, alarm_rule_id, alarm_rule, validation_metadata)
        perf_log("monitor-alert-check_if_alerted", ngx_now() - start_time2)
        if alert then
            -- 开始告警
            local entry = get_logger_entry(conf, ctx, alarm_rule, log_entry)
            --send_to_elasticsearch(conf, { entry })
            if batch_processor_manager:add_entry(conf, entry) then
                return
            end

            local process = function(entries)
                return send_to_elasticsearch(conf, entries)
            end

            batch_processor_manager:add_entry_to_new_processor(conf, entry, ctx, process)
        end

        ::CONTINUE::
    end

    perf_log("monitor-alert-async_check", ngx_now() - start_time)
end


function _M.init()
    read_redis_conf(cluster_info)
end


function _M.log(conf, ctx)
    --core.log.warn("xgz【conf】=>", core.json.encode(conf, true))
    --core.log.warn("xgz【ctx】=>", core.json.encode(ctx, true))

    if not conf.all_alarm_rule or type(conf.all_alarm_rule) ~= "table" or core.table.isempty(conf.all_alarm_rule) then
        return
    end

    local route_id = ctx.route_id
    if not route_id then
        return
    end

    init_plugin_metadata(conf, cluster_info)

    local log_entry = log_util.get_log_entry(plugin_name, conf, ctx)

    local validation_metadata = {
        -- 调用时间（单位：毫秒级时间戳）
        call_time = log_entry.start_time,
        -- 响应状态码
        response_status = ngx.status,
        -- 流入流量（单位字节）
        inbound_traffic = ctx.var.request_length,
        -- 延迟时间（单位毫秒）
        latency = log_entry.latency
    }

    core.log.warn("xgz【normal】validation_metadata=>",  core.json.encode(validation_metadata, true))

    local my_ctx = deepcopy(ctx)
    my_ctx.var.route_id = route_id
    my_ctx.var.server_addr = ctx.var.server_addr
    local ok, err = ngx.timer.at(0, async_check, conf, my_ctx, validation_metadata, log_entry)
    if not ok then
        core.log.error(ngx.ERR, "Failed to create timer: ", err)
    end
end


return _M
