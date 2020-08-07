#!/bin/bash
base_dir=/home/tico/zhijieliao/ssj_syyyw_ctr_wdl_v2
data_input_path=tf_data
#paramters
start=$1
gap=$2
date_gap=${gap:-7} 
echo date_gap : $date_gap
end=`date -d "${start} ${date_gap} days ago" +%Y%m%d`
keep_date=`date -d "${start} 7 days ago" +%Y%m%d`
train_file=tf_train_${start}
echo ------------------------------------------------
echo start union train data from ${start} until ${end}
#echo start date is $start
first_file=tf_${start}
echo $first_file
cat ${base_dir}/${data_input_path}/$first_file > ${base_dir}/tf_training_data/train_file
start=`date -d "-1 day ${start}" +%Y%m%d`
while [ ${start} -gt ${end} ]
do
  #echo ${start}
  file_name=tf_${start}
  echo $file_name
  cat ${base_dir}/${data_input_path}/$file_name >> ${base_dir}/tf_training_data/train_file
  start=`date -d "-1 day ${start}" +%Y%m%d`      # 日期自减
done
echo 'finish union'
echo -e '-------------------------------------------\nstart shuffle'
shuf ${base_dir}/tf_training_data/train_file -o ${base_dir}/tf_training_data/train_file_shuffle
rm ${base_dir}/tf_training_data/train_file
echo -e  'finish shuffle\n--------------------------------------------'

echo -e 'split file to train and test'
file_len=`cat ${base_dir}/tf_training_data/train_file_shuffle | wc -l`
test_len=$(echo "${file_len} * 1/10"|bc)
echo file_len is $file_len  test_len is $test_len
tail -n $test_len ${base_dir}/tf_training_data/train_file_shuffle  > ${base_dir}/tf_training_data/train_file_shuffle_test
head -n -$test_len ${base_dir}/tf_training_data/train_file_shuffle  > ${base_dir}/tf_training_data/train_file_shuffle_train
rm ${base_dir}/tf_training_data/train_file_shuffle
echo -e 'split train and test finish \n----------------------------------------'

#echo 'compress the file use Gzip'

#gzip -f ${base_dir}/tf_training_data/train_file_shuffle_train
#gzip -f ${base_dir}/tf_training_data/train_file_shuffle_test
#echo -e 'finish compress\n--------------------------------------------'

echo  rm $keep_date day ago files
for dir in $(ls $base_dir/${data_input_path})
do
  file_date=`echo $dir|cut -d "_" -f 2`
  #echo $file_date
  if [ $file_date -lt $keep_date ]
      then echo -e "rm ${dir}"
      rm $base_dir/${data_input_path}/${dir}
  fi
done
