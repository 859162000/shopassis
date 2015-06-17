#!/bin/sh
source /home/hadoop/.bash_profile
source /home/hadoop/lsj/app_data_assistant2.0/conf/configuration.conf


run_path=$__RUN_PATH
bin_path=$run_path/bin
data_path=$run_path/data
log_path=$run_path/log
tmp_path=$run_path/tmp
lib_path=$run_path/lib


now_timestamp=`date +%s`
#now_timestamp=`date -d'2015-5-3 00:00:00' +%s`

if [ $1 ];then
now_timestamp=$1
fi


hour=`date -d@$now_timestamp +%H`
if [ $hour -eq 0 ];then
now_timestamp=`perl -e "print $now_timestamp-3600"`
fi


today=`date -d@$now_timestamp +%Y%m%d`
hour=`date -d@$now_timestamp +%H`


cd $tmp_path

cd $data_path


cd $log_path

#Step 1 Extract
/bin/sh $bin_path/step1_Extract.sh $now_timestamp > $log_path/$today-$hour-step1_Extract.log 2>&1

wait

#Step 2 Transform
/bin/sh $bin_path/step2_Transform.sh $now_timestamp > $log_path/$today-$hour-step2_Transform.log 2>&1

wait

#Step 3 Load
/bin/sh $bin_path/step3_Load.sh $now_timestamp > $log_path/$today-$hour-step3_Load.log 2>&1

