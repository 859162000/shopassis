#!/bin/sh
source /home/hadoop/.bash_profile
source /home/hadoop/lsj/app_data_assistant2.0/conf/configuration.conf


run_path=$__RUN_PATH
bin_path=$run_path/bin
data_path=$run_path/data
log_path=$run_path/log
tmp_path=$run_path/tmp
lib_path=$run_path/lib

log_partition_type=$__LOG_PARTITION_TYPE

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

#all log calculation
cat $bin_path/step2_all_log.sql > $tmp_path/$today-$hour-step2_all_log.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step2_all_log.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step2_all_log.sql

#all order calculation
cat $bin_path/step2_all_order.sql > $tmp_path/$today-$hour-step2_all_order.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step2_all_order.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step2_all_order.sql

#category log calculation
cat $bin_path/step2_category_log.sql > $tmp_path/$today-$hour-step2_category_log.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step2_category_log.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step2_category_log.sql

#category order calculation
cat $bin_path/step2_category_order.sql > $tmp_path/$today-$hour-step2_category_order.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step2_category_order.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step2_category_order.sql

#channel log calculation
cat $bin_path/step2_channel_log.sql > $tmp_path/$today-$hour-step2_channel_log.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step2_channel_log.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step2_channel_log.sql

#channel order calculation
cat $bin_path/step2_channel_order.sql > $tmp_path/$today-$hour-step2_channel_order.sql
sed -i 's/{dt}/'$today'/g' $tmp_path/$today-$hour-step2_channel_order.sql
sed -i 's/{hour}/'$hour'/g' $tmp_path/$today-$hour-step2_channel_order.sql

echo $0'-step2_01 Start:'`date` >> $log_path/$today-$hour-log.log

echo "hive -f $tmp_path/$today-$hour-step2_all_log.sql" > $tmp_path/$today-$hour-step2_all_log.sh
echo "hive -f $tmp_path/$today-$hour-step2_all_order.sql" > $tmp_path/$today-$hour-step2_all_order.sh
echo "hive -f $tmp_path/$today-$hour-step2_category_log.sql" > $tmp_path/$today-$hour-step2_category_log.sh
echo "hive -f $tmp_path/$today-$hour-step2_category_order.sql" > $tmp_path/$today-$hour-step2_category_order.sh
echo "hive -f $tmp_path/$today-$hour-step2_channel_log.sql" > $tmp_path/$today-$hour-step2_channel_log.sh
echo "hive -f $tmp_path/$today-$hour-step2_channel_order.sql" > $tmp_path/$today-$hour-step2_channel_order.sh

/bin/sh $tmp_path/$today-$hour-step2_all_log.sh > $log_path/$today-$hour-step2_all_log.log 2>&1 &
/bin/sh $tmp_path/$today-$hour-step2_all_order.sh > $log_path/$today-$hour-step2_all_order.log 2>&1 &
/bin/sh $tmp_path/$today-$hour-step2_category_log.sh > $log_path/$today-$hour-step2_category_log.log 2>&1 &
/bin/sh $tmp_path/$today-$hour-step2_category_order.sh > $log_path/$today-$hour-step2_category_order.log 2>&1 &
/bin/sh $tmp_path/$today-$hour-step2_channel_log.sh > $log_path/$today-$hour-step2_channel_log.log 2>&1 &
/bin/sh $tmp_path/$today-$hour-step2_channel_order.sh > $log_path/$today-$hour-step2_channel_order.log 2>&1 &

wait

echo $0'-step2_01 Finish:'`date` >> $log_path/$today-$hour-log.log

