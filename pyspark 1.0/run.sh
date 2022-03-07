#!/bin/sh

source ./$1

source /etc/profile
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djline.terminal=jline.UnsupportedTerminal"
export HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native"
today=`date +%Y%m%d`

    /data/cloudera/parcels/CDH/bin/beeline -u "jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n liaozhijie -p fr#7kvj -f ${sql_file_part1}

    /data/spark/spark-1.6.0/bin/spark-submit \
        --name ad_data_joining_lht_build_feature \
        --driver-memory 4G \
        --executor-memory 3G \
        --num-executors 100 \
        --master yarn \
        --deploy-mode client \
        --queue ${spark_queue} ${py_file_1} temp.ad_ssj_request_show_click_log_for_build_timefeature temp.ad_ssj_request_show_clicklog_for_build_timefeature_new

    /data/cloudera/parcels/CDH/bin/beeline -u "jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n liaozhijie -p fr#7kvj -f ${sql_file_part3}

    /data/spark/spark-1.6.0/bin/spark-submit \
        --name update_ad_ssj_time_feature \
        --driver-memory 6G \
        --executor-memory 3G \
        --num-executors 100 \
        --master yarn \
        --deploy-mode client \
        --queue ${spark_queue} ${py_file_2}
