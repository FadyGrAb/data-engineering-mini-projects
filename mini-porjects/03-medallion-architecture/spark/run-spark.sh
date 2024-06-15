#!/bin/bash

if [ "$SPARK_MODE" = "master" ];
then
    echo "master"
    source ~/.bashrc
    /spark/sbin/start-master.sh
    mkdir -p /tmp/spark-events
    /spark/sbin/start-history-server.sh
    jupyter notebook --allow-root &
else
    echo "worker"
    /spark/sbin/start-worker.sh "spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT" -m "$WORKER_MEMORY" -c "$WORKER_CORES"
fi

tail -f /dev/null
