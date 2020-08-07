#!/bin/bash
date=$1
base_dir=/home/tico/zhijieliao/ssj_syyyw_ctr_wdl_v2
hive_data_dir=$base_dir/hive_data
tf_data_dir=$base_dir/tf_data
model_config_dir=$base_dir/model_config
#train_hive_db=intelli_algo
#train_hive_table=dw_ad_jiabin_ssj_syyyw_tf_data_v2
#train_hive_table=ssj_sp_syyy_model_train_data_v4

train_hive_db=temp
train_hive_table=wdl_add_new_feature_table

config_db=dw
config_table=bdl_bd_onlinead_viewlog
hive_file=hive_$date

partner_file=partner_$date
origid_file=origid_$date
num_date=`date -d "$date" +%Y%m%d`
tf_file=tf_$num_date
echo -e '---------------------------------------\n'
echo training date is $date
echo -e '---------------------------------------\n'
echo start get hive data ymd=$date
echo -e '---------------------------------------\n'
/data/cloudera/parcels/CDH/bin/beeline -u "jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n fdbd_ad -p vbbtar72 --showHeader=false  --incremental=true --outputformat=dsv --delimiterForDSV="#" -e "select * from ${train_hive_db}.${train_hive_table} where ymd='$date'" > ${tf_data_dir}/$tf_file
echo -e '---------------------------------------\n'
echo start get partner data ymd=$date
echo -e '---------------------------------------\n'
/data/cloudera/parcels/CDH/bin/beeline -u "jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n fdbd_ad -p vbbtar72 --showHeader=false  --incremental=true --outputformat=dsv --delimiterForDSV="#" -e "select a.partner from (select partner,count(1) as times from  ${config_db}.${config_table} where ymd='$date' and positionid in('SSJSYYYW','SP') group by partner order by times desc limit 100) as a;" > ${model_config_dir}/partner_config
echo -e '---------------------------------------\n'
echo start get origid data ymd=$date
echo -e '---------------------------------------\n'
/data/cloudera/parcels/CDH/bin/beeline -u "jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n fdbd_ad -p vbbtar72 --showHeader=false  --incremental=true --outputformat=dsv --delimiterForDSV="#" -e "select a.origid from (select origid,count(1) from  ${config_db}.${config_table} where ymd='$date' and positionid in('SSJSYYYW','SP') group by origid) as a;" > ${model_config_dir}/origid_config
echo -e '----------------------------------------\n'
#echo start clean data tf_$date
#tail -n +3 ${hive_data_dir}/${hive_file} | python ${base_dir}/beeline_process.py $train_hive_table > ${tf_data_dir}/$tf_file
#tail -n +3 ${hive_data_dir}/${partner_file} | python ${base_dir}/beeline_process.py $config_table > ${model_config_dir}/partner_config
#tail -n +3 ${hive_data_dir}/${origid_file} | python ${base_dir}/beeline_process.py $config_table > ${model_config_dir}/origid_config
#echo finsih clean data
#echo 'rm hive data'
#rm ${hive_data_dir}/*
