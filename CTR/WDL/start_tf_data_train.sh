#!/bin/bash
source /home/tico/.bash_profile 
default_date=$(date  +"%Y-%m-%d" -d  "-1 days")
input_date=$1
date=${input_date:-$default_date}
date_format=$(date -d "$date" +"%Y%m%d")
echo data time is $date_format
base_dir=/home/tico/zhijieliao/ssj_syyyw_ctr_wdl_v2
monitor_text=$base_dir/monitor_data
#target_table=intelli_algo.dw_ad_jiabin_ssj_syyyw_tf_data_v2
#target_table=intelli_algo.ssj_sp_syyy_model_train_data_v4
target_table=temp.wdl_add_new_feature_table

    /data/cloudera/parcels/CDH/bin/beeline -u "jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n intelli_algo_ad -p wq5x#e3t -f create_new_feature_table.sql

    /data/cloudera/parcels/CDH/bin/beeline -u "jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n intelli_algo_ad -p wq5x#e3t --showHeader=false  --incremental=true --outputformat=dsv --delimiterForDSV="#" -e "select * from ${target_table} where ymd='$date' limit 50;" > ${monitor_text}

monitor_data_len=`cat ${base_dir}/monitor_data | wc -l`
echo $monitor_data_len
if [ $monitor_data_len -gt 10 ]; then
        echo 'this is exist'
	#sh $base_dir/create_7_day_data.sh
        sh $base_dir/data_clean.sh $date
	sh $base_dir/create_data.sh $date_format
	sh $base_dir/model_train.sh $date_format
        sh $base_dir/get_wdl_weight.sh $date
	current_ver_log=$(tail -1 ${base_dir}/trainLogs/${date_format}.log)
	python $base_dir/email_monitor.py "${base_dir} ${current_ver_log}"
else
	echo -e "ERROR The ${target_table} partition $date not exist"
        python $base_dir/email_monitor.py "ERROR The ${target_table} partition $date not exist"
fi
