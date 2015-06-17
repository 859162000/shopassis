
SET mapreduce.job.priority=HIGH;

use ods_new;

from 
(
select 
hour,
center_id,
case
	when product_id like 'A%' then 'ly' 
	else 'zy' 
end as p_type,
category1_id,
category2_id,
category3_id,
page_url_tag,
user_cookie_id,
session_id,
is_real,
is_exit,
is_jump,
last_access_time as sum_access_time,
case 
	when page_url_tag[2] = 'product' then 1 
	else 0 
end as is_product,
case 
	when type_id = 1 then 1 
	else 0  
end as is_shop_cart 
from ods_new.sitemonitor_log 
where dt = {dt}{hour} and type = 'step3_done' and log_type = '0' and index = '1' 
) tbl 


insert overwrite table ods_new.report_app_center_flow partition ( dt = {dt}{hour} , type = 'center_other' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id as center_id,
page_url_tag[2] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
sum(is_exit) as exit_num,
count(distinct session_id) as visits_num,
sum(sum_access_time) as sum_access_time,
sum(is_product) as ipv,
sum(is_shop_cart) as shop_cart_num,
sum(is_product) as product_click_num,
case 
	when page_url_tag[2] = 'supermarket' then 1 
	when page_url_tag[2] = 'list' then 2 
	when page_url_tag[2] = 'product' then 4 
	else 0 
end as dim_value 
where center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' 
group by hour,center_id,page_url_tag[2] 


insert overwrite table ods_new.report_app_center_flow partition ( dt = {dt}{hour} , type = 'center_ylsh' ) 
select 
{dt} as report_date,
hour as report_hour,
'ylsh' as center_id,
page_url_tag[0] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
sum(is_exit) as exit_num,
count(distinct session_id) as visits_num,
sum(sum_access_time) as sum_access_time,
sum(is_product) as ipv,
sum(is_shop_cart) as shop_cart_num,
sum(is_product) as product_click_num,
case 
	when page_url_tag[0] = 'chongzhi.gome.com.cn' then 1 
	when page_url_tag[0] = 'caipiao.gome.com.cn' then 2 
	when page_url_tag[0] = 'game.gome.com.cn' then 4 
	else 0 
end as dim_value 
where center_id = 'ylsh' or ( page_url_tag[0] = 'chongzhi.gome.com.cn' or page_url_tag[0] = 'caipiao.gome.com.cn' or page_url_tag[0] = 'game.gome.com.cn' ) 
group by hour,page_url_tag[0] 


insert overwrite table ods_new.report_app_center_flow partition ( dt = {dt}{hour} , type = 'center_lyjr' ) 
select 
{dt} as report_date,
hour as report_hour,
'lyjr' as center_id,
page_url_tag[0] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
sum(is_exit) as exit_num,
count(distinct session_id) as visits_num,
sum(sum_access_time) as sum_access_time,
sum(is_product) as ipv,
sum(is_shop_cart) as shop_cart_num,
sum(is_product) as product_click_num,
case 
	when page_url_tag[0] = 'jr.gome.com.cn' then 1 
	when page_url_tag[0] = 'piaoju.gome.com.cn' then 2 
	when page_url_tag[0] = 'jipiao.gome.com.cn' then 4 
	else 0 
end as dim_value 
where center_id = 'lyjr' or ( page_url_tag[0] = 'jr.gome.com.cn' or page_url_tag[0] = 'piaoju.gome.com.cn' or page_url_tag[0] = 'jipiao.gome.com.cn' ) 
group by hour,page_url_tag[0] 



insert overwrite table ods_new.report_app_center_flow partition ( dt = {dt}{hour} , type = 'center_tg' ) 
select 
{dt} as report_date,
hour as report_hour,
'tg' as center_id,
category3_id as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
sum(is_exit) as exit_num,
count(distinct session_id) as visits_num,
sum(sum_access_time) as sum_access_time,
sum(is_product) as ipv,
sum(is_shop_cart) as shop_cart_num,
sum(is_product) as product_click_num,
1 as dim_value 
where center_id = 'tg' or ( page_url_tag[0] = 'tuan.gome.com.cn' ) 
group by hour,category3_id 



--center_sale
insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'center_c1_log' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id,
category1_id as dim1,
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
case 
	when p_type = 'zy' then 1 
	else 8 
end as dim_value 
where is_product =1 and ( center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' ) 
group by hour,center_id,p_type,category1_id 


insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'center_c1_c2' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id,
category1_id as dim1,
category2_id as dim2,
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
case 
	when p_type = 'zy' then 2 
	else 16 
end as dim_value 
where is_product =1 and ( center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' ) 
group by hour,center_id,p_type,category1_id,category2_id 

insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'center_c1_c2_c3' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id,
category1_id as dim1,
category2_id as dim2,
category3_id as dim3,
sum(is_product) as ipv,
0 as online_product_num,
0 as order_num,
0 as order_product_num,
0 as saled_product_num,
0 as sum_money,
count(distinct user_cookie_id) as uv,
0 as cancel_order_num,
sum(is_product) as product_click_num,
case 
	when p_type = 'zy' then 4 
	else 32 
end as dim_value 
where is_product =1 and ( center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' ) 
group by hour,center_id,p_type,category1_id,category2_id,category3_id  ;












