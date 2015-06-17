
SET mapreduce.job.priority=HIGH;

use ods_new;

insert overwrite table ods_new.report_app_date partition ( dt = {dt}{hour} , type = 'all' ) 
select 
report_date,
report_hour,
sum(pv),
sum(uv),
sum(real_uv),
sum(shop_cart_uv),
sum(submit_uv),
sum(sum_money),
sum(order_num) 
from ods_new.report_app_date 
where dt = {dt}{hour} and type <> 'all' 
group by report_date,report_hour ;
