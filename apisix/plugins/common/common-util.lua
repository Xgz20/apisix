---
--- @description: 通用的工具方法
--- @author: xgz
--- @date: 2023/9/11
---

local core             = require("apisix.core")
local fetch_local_conf = require("apisix.core.config_local").local_conf
local table            = require("apisix.core.table")
local redis_new        = require("resty.redis").new
local rediscluster     = require("resty.rediscluster")
local ngx_now          = ngx.now
local str_format       = core.string.format


local _M = {}


local function _perf_log(process_name, ...)
    local is_open = false
    if is_open then
        local log_type = "perf_test"
        core.log.notice(str_format("xgz【%s】【%s】cost time =>", log_type, process_name), ...)
    end
end

_M.perf_log = _perf_log


local rio_default_header = {
    paasid = "x-rio-paasid",
    nonce = "x-rio-nonce",
    timestamp = "x-rio-timestamp",
    signature = "x-rio-signature"
}


local function _get_header_value(header_flag, headers)
    local start_time = ngx_now()
    local local_conf = fetch_local_conf()
    local header_value
    if local_conf.rio then
        local rio_headers = table.try_read_attr(local_conf, "rio", "header", header_flag)
        --core.log.warn("xgz【normal】rio_headers=>", core.json.encode(rio_headers, true))
        if rio_headers then
            for _, header_name in ipairs(rio_headers) do
                header_value = headers[header_name]
                if header_value then
                    break
                end
            end
        end
    end

    if header_value then
        _perf_log("get_header_value【" .. header_flag .. "】", ngx_now() - start_time)

        return header_value
    else
        local result = headers[rio_default_header[header_flag]]
        _perf_log("get_header_value【" .. header_flag .. "】", ngx_now() - start_time)

        return result
    end

end

_M.get_header_value = _get_header_value


function _M.get_subscriber_id(ctx)
    local req_key = ctx.subscriber_id
    core.log.notice("xgz【normal】ctx.subscriber_key =>", req_key)

    if not req_key then
        local headers = ctx.headers
        if not headers then
            headers = core.request.headers(ctx)
        end
        req_key = _get_header_value("paasid", headers)
        if req_key then
            ctx.subscriber_id = req_key
        else
            return nil
        end
    end
    core.log.notice("xgz【normal】req_key =>", req_key)
    return req_key
end


function _M.intercept_result(ctx, rio_rsp_body)
    ctx.rio_rsp_body = rio_rsp_body
    return 200, rio_rsp_body
end


local function new_redis(conf)
    core.log.notice("xgz【normal】=>", "create redis-cli")
    --core.log.warn("xgz【conf】=>", core.json.encode(conf, true))
    local red = redis_new()
    local timeout = conf.redis_timeout or 1000    -- 1sec

    red:set_timeouts(timeout, timeout, timeout)

    local ok, err = red:connect(conf.redis_host, conf.redis_port or 6379)
    if not ok then
        return false, err
    end

    local count
    count, err = red:get_reused_times()
    if 0 == count then
        if conf.redis_password and conf.redis_password ~= '' then
            local ok, err = red:auth(conf.redis_password)
            if not ok then
                return nil, err
            end
        end

        -- select db
        if conf.redis_database ~= 0 then
            local ok, err = red:select(conf.redis_database)
            if not ok then
                return false, "failed to change redis db, err: " .. err
            end
        end
    elseif err then
        -- core.log.info(" err: ", err)
        return nil, err
    end
    return red, nil
end


local function new_redis_cluster(conf)
    core.log.notice("xgz【normal】=>", "create redis-cluster")

    local config = {
        -- can set different name for different redis cluster
        name = conf.redis_cluster_name,
        serv_list = {},
        read_timeout = conf.redis_timeout,
        --connect_timeout = conf.redis_timeout,
        --send_timeout = conf.redis_timeout,
        auth = conf.redis_password,
        dict_name = "hermes-redis-cluster-slot-lock",
        connect_opts = {
            ssl = conf.redis_cluster_ssl,
            ssl_verify = conf.redis_cluster_ssl_verify,
        }
    }

    for i, conf_item in ipairs(conf.redis_cluster_nodes) do
        local host, port, err = core.utils.parse_addr(conf_item)
        if err then
            return nil, "failed to parse address: " .. conf_item
                    .. " err: " .. err
        end

        config.serv_list[i] = {ip = host, port = port}
    end

    local red_cli, err = rediscluster:new(config)
    if not red_cli then
        return nil, "failed to new redis cluster: " .. err
    end

    return red_cli
end


function _M.redis_cli(conf)
    --core.log.warn("xgz【normal】conf=>", core.json.encode(conf, true))
    if conf.redis_policy ~= "redis" then
        return new_redis_cluster(conf)
    else
        return new_redis(conf)
    end
end


local policy_to_additional_properties = {
    redis = {
        properties = {
            redis_host = {
                type = "string", minLength = 2
            },
            redis_port = {
                type = "integer", minimum = 1, default = 6379,
            },
            redis_password = {
                type = "string", minLength = 0,
            },
            redis_database = {
                type = "integer", minimum = 0, default = 0,
            },
            redis_timeout = {
                type = "integer", minimum = 1, default = 1000,
            },
        },
        required = {"redis_host"},
    },
    ["redis-cluster"] = {
        properties = {
            redis_cluster_nodes = {
                type = "array",
                minItems = 2,
                items = {
                    type = "string", minLength = 2, maxLength = 100
                },
            },
            redis_password = {
                type = "string", minLength = 0,
            },
            redis_timeout = {
                type = "integer", minimum = 1, default = 1000,
            },
            redis_cluster_name = {
                type = "string",
            },
            redis_cluster_ssl = {
                type = "boolean", default = false,
            },
            redis_cluster_ssl_verify = {
                type = "boolean", default = false,
            },
        },
        required = {"redis_cluster_nodes", "redis_cluster_name"},
    },
}

local redis_attr_schema = {
    properties = {
        redis_policy = {
            type = "string",
            enum = {"redis", "redis-cluster"},
            default = "redis-cluster",
        },
    },
    ["if"] = {
        properties = {
            redis_policy = {
                enum = {"redis"},
            },
        },
    },
    ["then"] = policy_to_additional_properties.redis,
    ["else"] = {
        ["if"] = {
            properties = {
                redis_policy = {
                    enum = {"redis-cluster"},
                },
            },
        },
        ["then"] = policy_to_additional_properties["redis-cluster"],
    }
}

function _M.read_redis_conf(cluster_info)
    local local_conf = core.config.local_conf()
    local deploy_mode = core.table.try_read_attr(local_conf, "deploy", "mode")
    core.log.warn("deploy mode =>", deploy_mode)
    cluster_info.deploy_mode = deploy_mode

    if deploy_mode and deploy_mode ~= "local" then
        cluster_info.attr = core.table.try_read_attr(local_conf, "cluster_conf", "redis")
        core.log.warn("attr =>", core.json.encode(cluster_info.attr, true))
        local ok, err = core.schema.check(redis_attr_schema, cluster_info.attr)
        if not ok then
            core.log.error("failed to check the redis conf: ", err)
            core.log.error("cluster mode must config redis, please check the redis conf in config.yaml or config-default.yaml")
            return
        end

        if cluster_info.attr then
            core.log.warn("redis mode =>", cluster_info.attr.redis_policy)
        end

        -- Redis服务可用性检查
        -- TODO：Redis集群模式中存在lock API不能在此init阶段执行，Redis单机没问题
        --[[
        local red, err_info = _M.redis_cli(cluster_info.attr)
        if not red then
            core.log.error("create redis-cli failed, reason =>", err_info)
            return
        end
        local pong = red:ping()
        if not pong then
            core.log.error("redis server is not available")
            return
        end
        --]]
    end
end


function _M.init_plugin_metadata(conf, cluster_info)
    conf.policy = cluster_info.deploy_mode or "local"
    if cluster_info.attr then
        local redis_policy = cluster_info.attr.redis_policy
        if redis_policy ~= "redis" then
            -- 默认是Redis集群
            conf.redis_policy = redis_policy
            conf.redis_cluster_nodes = cluster_info.attr.redis_cluster_nodes
            conf.redis_password = cluster_info.attr.redis_password
            conf.redis_timeout = cluster_info.attr.redis_timeout
            conf.redis_cluster_name = cluster_info.attr.redis_cluster_name
            conf.redis_cluster_ssl = cluster_info.attr.redis_cluster_ssl
            conf.redis_cluster_ssl_verify = cluster_info.attr.redis_cluster_ssl_verify
        else
            -- Redis单机
            conf.redis_policy = redis_policy
            conf.redis_host = cluster_info.attr.redis_host
            conf.redis_port = cluster_info.attr.redis_port
            conf.redis_password = cluster_info.attr.redis_password
            conf.redis_database = cluster_info.attr.redis_database
            conf.redis_timeout = cluster_info.attr.redis_timeout
        end

    end
end


return _M
