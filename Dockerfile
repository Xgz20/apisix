FROM artifacts.iflytek.com/stc-docker-private/stcapiwgv40/apisix:3.2.1-centos-multiarch
COPY ./apisix/plugins/common/common-util.lua /usr/local/apisix/apisix/plugins/common/common-util.lua
COPY ./apisix/plugins/subscribe.lua /usr/local/apisix/apisix/plugins/subscribe.lua
COPY ./apisix/plugins/sign-auth.lua /usr/local/apisix/apisix/plugins/sign-auth.lua

COPY ./apisix/plugins/limit-conn-by-subscriber.lua /usr/local/apisix/apisix/plugins/limit-conn-by-subscriber.lua
COPY ./apisix/plugins/limit-conn/limit-conn-local.lua /usr/local/apisix/apisix/plugins/limit-conn/limit-conn-local.lua
COPY ./apisix/plugins/limit-conn/limit-conn-redis.lua /usr/local/apisix/apisix/plugins/limit-conn/limit-conn-redis.lua

COPY ./apisix/plugins/limit-count-by-subscriber.lua /usr/local/apisix/apisix/plugins/limit-count-by-subscriber.lua
COPY ./apisix/plugins/limit-count/limit-count-local-subscriber.lua /usr/local/apisix/apisix/plugins/limit-count/limit-count-local-subscriber.lua
COPY ./apisix/plugins/limit-count/limit-count-redis-subscriber.lua /usr/local/apisix/apisix/plugins/limit-count/limit-count-redis-subscriber.lua

COPY ./apisix/plugins/limit-conn-by-site.lua /usr/local/apisix/apisix/plugins/limit-conn-by-site.lua
COPY ./apisix/plugins/limit-count-by-site.lua /usr/local/apisix/apisix/plugins/limit-count-by-site.lua

COPY ./apisix/plugins/response-status-rewrite.lua /usr/local/apisix/apisix/plugins/response-status-rewrite.lua
COPY ./apisix/plugins/service-breaker.lua /usr/local/apisix/apisix/plugins/service-breaker.lua
COPY ./apisix/plugins/custom-es-logger.lua /usr/local/apisix/apisix/plugins/custom-es-logger.lua
COPY ./apisix/plugins/ip-blacklist.lua /usr/local/apisix/apisix/plugins/ip-blacklist.lua
COPY ./apisix/plugins/monitor-alert.lua /usr/local/apisix/apisix/plugins/monitor-alert.lua
COPY ./apisix/plugins/uri-whitelist.lua /usr/local/apisix/apisix/plugins/uri-whitelist.lua
COPY ./apisix/plugins/cross-network.lua /usr/local/apisix/apisix/plugins/cross-network.lua

COPY ./conf/config-default.yaml /usr/local/apisix/conf/config-default.yaml
