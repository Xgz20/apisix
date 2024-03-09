#!/bin/bash

chmod -R 755 ./apisix

docker-compose -f docker-compose-arm64.yml up -d
