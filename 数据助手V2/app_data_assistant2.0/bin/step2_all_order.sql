
SET mapreduce.job.priority=HIGH;

use ods_new;

from 
(
select 
submitted_date,
hour,
order_id,
product_id,
amount,
quantity,
prd_brand,
center_id,
category_id_1,
category_id_2,
category_id_3,
user_id,
pay_success_flag,
channel_tag[0] as channel,
lpg_type 
from ods_new.app_atg_order 
where dt = {dt}{hour} and type = 'sm_order' and pay_success_flag = 1
) tbl 

--report_app_date
insert overwrite table ods_new.report_app_date partition ( dt = {dt}{hour} , type = 'order_daily' ) 
select 
{dt} as report_date,
-1 as report_hour,
0 as pv,
0 as uv,
0 as real_uv,
0 as shop_cart_uv,
0 as submit_uv,
sum(amount) as sum_money,
count(distinct order_id) as order_num 

insert overwrite table ods_new.report_app_date partition ( dt = {dt}{hour} , type = 'order_hourly' ) 
select 
{dt} as report_date,
hour as report_hour,
0 as pv,
0 as uv,
0 as real_uv,
0 as shop_cart_num,
0 as submit_uv,
sum(amount) as sum_money,
count(distinct order_id) as order_num 
group by hour 


--report_app_channel
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'order_daily' ) 
select 
{dt} as report_date,
-1 as report_hour,
channel,
'' as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
0 as dim_value 
group by channel 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'order_hourly' ) 
select 
{dt} as report_date,
hour as report_hour,
channel,
'' as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
0 as dim_value 
group by channel,hour 


--report_app_center_flow

--report_app_center_sale
insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'order_daily' ) 
select 
{dt} as report_date,
-1 as report_hour,
center_id,
'' as dim1,
'' as dim2,
'' as dim3,
0 as ipv,
0 as online_product_num,
count(distinct order_id) as order_num,
0 as order_product_num,
0 as saled_product_num,
sum(amount) as sum_money,
0 as uv,
0 as cancel_order_num,
0 as product_click_num,
0 as dim_value 
group by center_id 

insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'order_hourly' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id,
'' as dim1,
'' as dim2,
'' as dim3,
0 as ipv,
0 as online_product_num,
count(distinct order_id) as order_num,
sum(quantity) as order_product_num,
count(product_id) as saled_product_num,
sum(amount) as sum_money,
0 as uv,
0 as cancel_order_num,
0 as product_click_num,
0 as dim_value 
group by center_id,hour;


