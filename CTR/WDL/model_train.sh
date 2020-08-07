#!/bin/bash
source /etc/profile
base_dir=/home/tico/zhijieliao/ssj_syyyw_ctr_wdl_v2
stat_date=$1
epoch=15
#source /home/zhijieliao/tensorflow1.10/bin/activate
source /usr/local/anaconda3/bin/activate tf_xla
#echo `which python`
python -u $base_dir/wideAndDeepTrain.py $epoch > $base_dir/trainLogs/${stat_date}.log 2>&1
conda deactivate
find $base_dir/model_check_online/ -maxdepth 1 -type d -mtime +4 | xargs -r rm -rf
find $base_dir/export_model/ -maxdepth 1 -type d -mtime +5 | xargs rm -rf
find $base_dir/trainLogs/ -maxdepth 1 -type f -mtime +5 | xargs -r rm 
