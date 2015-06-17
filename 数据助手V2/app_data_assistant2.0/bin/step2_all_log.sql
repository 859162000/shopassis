
SET mapreduce.job.priority=HIGH;

use ods_new;

from 
(
select 
{dt}{hour} as dt,
hour,
channel_tag[0] as channel,
center_id,
user_cookie_id,
case 
	when page_url_tag[2] = 'product' then 1 
	else 0 
end as is_product,
case 
	when is_real = 1 then 0 
	else 1 
end as is_real,
case 
	when type_id = 1 then user_cookie_id 
	else '' 
end as shop_cart_uc,
case when type_id = 52 then user_cookie_id 
	else '' 
end as submit_uc,
case 
	when log_type = '0' and index = '1' then 1 
	else 0 
end as pv_num,
is_jump,
is_exit,
session_id,
last_access_time as sum_access_time 
from ods_new.sitemonitor_log 
where dt = {dt}{hour} and type = 'step3_done' 
) tbl 

--report_app_date
insert overwrite table ods_new.report_app_date partition ( dt = {dt}{hour} , type = 'log_daily' ) 
select 
{dt} as report_date,
-1 as report_hour,
sum(pv_num) as pv,
count(distinct user_cookie_id) as uv,
count(distinct user_cookie_id) - sum(is_real) as real_uv,
count(distinct shop_cart_uc) as shop_cart_uv,
count(distinct submit_uc) as submit_uv,
0 as sum_money,
0 as order_num 

insert overwrite table ods_new.report_app_date partition ( dt = {dt}{hour} , type = 'log_hourly' ) 
select 
{dt} as report_date,
hour as report_hour,
sum(pv_num) as pv,
count(distinct user_cookie_id) as uv,
count(distinct user_cookie_id) - sum(is_real) as real_uv,
count(distinct shop_cart_uc) as shop_cart_num,
count(distinct submit_uc) as submit_uv,
0 as sum_money,
0 as order_num 
group by hour 


--report_app_channel
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'log_daily' ) 
select 
{dt} as report_date,
-1 as report_hour,
channel,
'' as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
sum(pv_num) as pv,
count(distinct user_cookie_id) - sum(is_real) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
0 as dim_value 
group by channel 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'log_hourly' ) 
select 
{dt} as report_date,
hour as report_hour,
channel,
'' as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
sum(pv_num) as pv,
count(distinct user_cookie_id) - sum(is_real) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
0 as dim_value 
group by channel,hour 


--report_app_center_flow
insert overwrite table ods_new.report_app_center_flow partition ( dt = {dt}{hour} , type = 'log_daily' ) 
select 
{dt} as report_date,
-1 as report_hour,
center_id,
'' as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
sum(pv_num) as pv,
sum(is_exit) as exit_num,
count(distinct session_id) as visits_num,
sum(sum_access_time),
sum(is_product) as ipv,
count(distinct shop_cart_uc) as shop_cart_num,
sum(is_product) as product_click_num,
0 as dim_value 
group by center_id 

insert overwrite table ods_new.report_app_center_flow partition ( dt = {dt}{hour} , type = 'log_hourly' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id,
'' as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
sum(pv_num) as pv,
sum(is_exit) as exit_num,
count(distinct session_id) as visits_num,
sum(sum_access_time),
sum(is_product) as ipv,
count(distinct shop_cart_uc) as shop_cart_num,
sum(is_product) as product_click_num,
0 as dim_value 
group by center_id,hour 


--report_app_center_sale
insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'log_daily' ) 
select 
{dt} as report_date,
-1 as report_hour,
center_id,
'' as dim1,
'' as dim2,
'' as dim3,
sum(is_product) as ipv,
0 as online_product_num,
0 as order_num,
0 as order_product_num,
0 as saled_product_num,
0 as sum_money,
count(distinct user_cookie_id) as uv,
0 as cancel_order_num,
sum(is_product) as product_click_num,
0 as dim_value 
group by center_id 

insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'log_hourly' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id,
'' as dim1,
'' as dim2,
'' as dim3,
sum(is_product) as ipv,
0 as online_product_num,
0 as order_num,
0 as order_product_num,
0 as saled_product_num,
0 as sum_money,
count(distinct user_cookie_id) as uv,
0 as cancel_order_num,
sum(is_product) as product_click_num,
0 as dim_value 
group by center_id,hour;


