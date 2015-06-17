
SET mapreduce.job.priority=HIGH;

use ods_new;

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'all' ) 
select 
report_date,
report_hour,
channel,
dim1,
dim2,
sum(uv),
sum(pv),
sum(real_uv),
sum(order_num),
sum(sum_money),
sum(jump_num),
dim_value 
from ods_new.report_app_channel 
where  dt = {dt}{hour} and type <> 'all' 
group by  report_date,report_hour,channel,dim1,dim2,dim_value ;

