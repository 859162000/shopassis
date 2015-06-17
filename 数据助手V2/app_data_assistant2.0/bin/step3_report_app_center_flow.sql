
SET mapreduce.job.priority=HIGH;

use ods_new;

insert overwrite table ods_new.report_app_center_flow partition ( dt = {dt}{hour} , type = 'all' ) 
select 
report_date,
report_hour,
center_id,
dim1,
dim2,
sum(uv),
sum(pv),
sum(exit_num),
sum(visits_num),
sum(sum_access_time),
sum(ipv),
sum(shop_cart_num),
sum(product_click_num),
dim_value 
from ods_new.report_app_center_flow 
where dt = {dt}{hour} and type <> 'all' 
group by report_date,report_hour,center_id,dim1,dim2,dim_value ;