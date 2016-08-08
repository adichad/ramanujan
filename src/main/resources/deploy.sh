#!/bin/bash

#curl -XGET 'http://search01.production.askme.com:9200/_cluster/health/askme_a?wait_for_status=green&timeout=30m'

sudo chmod 777 /tmp

cd ~
rm -f ramanujan*.tgz
scp search21.production.askme.com:/data1/ramanujan/origin/master/build/distributions/mandelbrot-0.1.0.tgz .
scp search21.production.askme.com:/data1/ramanujan/origin/master/dep/build/distributions/mandelbrot-dep-0.1.0.tgz .
scp search21.production.askme.com:/data1/ramanujan/origin/master/env/adichad/build/distributions/mandelbrot-env-awsprod-search01-0.1.0.tgz .

cd /apps
rm -rf /apps/mandelbrot-0.1.0
tar -xzvf ~/mandelbrot-0.1.0.tgz 
tar -xzvf ~/mandelbrot-dep-0.1.0.tgz
tar -xzvf ~/mandelbrot-env-awsprod-search01-0.1.0.tgz

#kill `cat /apps/logs/mandelbrot-awsprod-search01/mandelbrot-awsprod-search01.pid`
sysctl vm.max_map_count && sudo sysctl -w vm.max_map_count=262144000
sleep 3
#nohup /usr/bin/java -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseG1GC -XX:MaxGCPauseMillis=800 -XX:InitiatingHeapOccupancyPercent=80 -XX:G1ReservePercent=15 -XX:+DisableExplicitGC -d64 -javaagent:/apps/newrelic/newrelic.jar -cp /apps/mandelbrot-0.1.0/lib/mandelbrot-0.1.0.jar:/apps/mandelbrot-0.1.0/lib/* -Xms25g -Xmx25g -Dlog.level=INFO com.askme.mandelbrot.Launcher > /apps/mandelbrot-0.1.0/mandebrot.out &