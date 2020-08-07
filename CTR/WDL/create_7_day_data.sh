#!/bin/bash
date=$1
input_date=$1
default_date=$(date  +"%Y%m%d" -d  "-1 days")
date=${input_date:-$default_date}
day_before_week=`date -d "${traindate} 9 days ago " +%Y%m%d`
base_dir=/home/tico/zhijieliao/ssj_syyyw_ctr_wdl_v2
tf_data_dir=$base_dir/tf_data
model_config_dir=$base_dir/model_config
train_hive_db=intelli_algo
train_hive_table=ssj_sp_syyy_model_train_data_v4
train_hive_db=temp
train_hive_table=wdl_add_new_feature_table
config_db=dw
config_table=bdl_bd_onlinead_viewlog
partner_file=partner_$date
origid_file=origid_$date

#sql_new_table='use temp'
#/data/cloudera/parcels/CDH/bin/beeline -u "jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n fdbd_ad -p vbbtar72 --showHeader=false  --incremental=true --outputformat=dsv --delimiterForDSV="#" -e sql_new_table

echo ${day_before_week}
echo ${date}

while [ ${date} -gt ${day_before_week} ]
do
  num_date=`date -d "$date" +%Y%m%d`
  tf_file=tf_$num_date
  date_1=`date -d "${date} 0 days ago " +%Y-%m-%d`
  echo ${date_1}
  /data/cloudera/parcels/CDH/bin/beeline -u "jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n fdbd_ad -p vbbtar72 --showHeader=false  --incremental=true --outputformat=dsv --delimiterForDSV="#" -e "select * from ${train_hive_db}.${train_hive_table} where ymd='$date_1'" > ${tf_data_dir}/$tf_file
  file_name=tf_${date}
  echo $file_name
  date=`date -d "-1 day ${date}" +%Y%m%d`      # 日期自减
done
