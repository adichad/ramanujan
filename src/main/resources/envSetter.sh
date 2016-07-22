#!/usr/bin/bash

# the runner that sets up every thing pre GO!

nohup mysql.server start > /tmp/logs/internal_sql_server.logs 2>&1 &