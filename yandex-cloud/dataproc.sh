#! /usr/bin/bash
# для более простого запуска/остановки кластера в yc. 
# при запуске выводится публичный ip мастер ноды для удобства, 
# так как он каждый раз новый
cluster_name='dataproc214'
cluster_status=$(yc dataproc cluster get $cluster_name | grep status | awk '{print $2}')

if test "$cluster_status" = "STOPPED"; then
    yc dataproc cluster start $cluster_name
    master_ip=$(yc vpc addr list | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+')
    echo $master_ip
elif test "$cluster_status" = "RUNNING"; then
    yc dataproc cluster stop $cluster_name
fi
