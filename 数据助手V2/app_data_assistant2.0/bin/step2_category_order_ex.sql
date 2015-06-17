
SET mapreduce.job.priority=HIGH;

use ods_new;

from 
(
select 
submitted_date,
hour,
order_id,
product_id,
case 
	when product_id like 'A%' then 'ly' 
	else 'zy' 
end as p_type,
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
where dt = {dt}{hour} and type = 'sm_order' 
) tbl 

--center
insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'center_c1' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id,
category_id_1 as dim1,
'' as dim2,
'' as dim3,
0 as ipv,
0 as online_product_num,
count(distinct order_id) as order_num,
sum(quantity) as order_product_num,
count(distinct product_id) as saled_product_num,
sum(amount) as sum_money,
0 as uv,
count(distinct case when pay_success_flag = 0 then order_id else '' end) as cancel_order_num,
0 as product_click_num,
case 
	when p_type = 'zy' then 1 
	else 8 
end as dim_value 
where center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' 
group by hour,center_id,p_type,category_id_1 


insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'center_c1_c2' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id,
category_id_1 as dim1,
category_id_2 as dim2,
'' as dim3,
0 as ipv,
0 as online_product_num,
count(distinct order_id) as order_num,
sum(quantity) as order_product_num,
count(distinct product_id) as saled_product_num,
sum(amount) as sum_money,
0 as uv,
count(distinct case when pay_success_flag = 0 then order_id else '' end) as cancel_order_num,
0 as product_click_num,
case 
	when p_type = 'zy' then 2 
	else 16 
end as dim_value 
where center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' 
group by hour,center_id,p_type,category_id_1,category_id_2 

insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'center_c1_c2_c3' ) 
select 
{dt} as report_date,
hour as report_hour,
center_id,
category_id_1 as dim1,
category_id_2 as dim2,
category_id_3 as dim3,
0 as ipv,
0 as online_product_num,
count(distinct order_id) as order_num,
sum(quantity) as order_product_num,
count(distinct product_id) as saled_product_num,
sum(amount) as sum_money,
0 as uv,
count(distinct case when pay_success_flag = 0 then order_id else '' end) as cancel_order_num,
0 as product_click_num,
case 
	when p_type = 'zy' then 4 
	else 32 
end as dim_value 
where center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' 
group by hour,center_id,p_type,category_id_1,category_id_2,category_id_3 
;





from 
(
select 
case 
	when p_id like 'A%' then 'ly' 
	else 'zy' 
end as p_type,
p_id,
center_id,
c1_id as category_id_1,
c2_id as category_id_2,
c3_id as category_id_3 
from ods_new.dim_product 
where is_sale = 1 
) tbl 

insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'center_c1_op' ) 
select 
{dt} as report_date,
-2 as report_hour,
center_id,
category_id_1 as dim1,
'' as dim2,
'' as dim3,
0 as ipv,
count(distinct p_id) as online_product_num,
0 as order_num,
0 as order_product_num,
0 as saled_product_num,
0 as sum_money,
0 as uv,
0 as cancel_order_num,
0 as product_click_num,
case 
	when p_type = 'zy' then 1 
	else 8 
end as dim_value 
where center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' 
group by center_id,p_type,category_id_1 


insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'center_c1_c2_op' ) 
select 
{dt} as report_date,
-2 as report_hour,
center_id,
category_id_1 as dim1,
category_id_2 as dim2,
'' as dim3,
0 as ipv,
count(distinct p_id) as online_product_num,
0 as order_num,
0 as order_product_num,
0 as saled_product_num,
0 as sum_money,
0 as uv,
0 as cancel_order_num,
0 as product_click_num,
case 
	when p_type = 'zy' then 2 
	else 16 
end as dim_value 
where center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' 
group by center_id,p_type,category_id_1,category_id_2 

insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'center_c1_c2_c3_op' ) 
select 
{dt} as report_date,
-2 as report_hour,
center_id,
category_id_1 as dim1,
category_id_2 as dim2,
category_id_3 as dim3,
0 as ipv,
count(distinct p_id) as online_product_num,
0 as order_num,
0 as order_product_num,
0 as saled_product_num,
0 as sum_money,
0 as uv,
0 as cancel_order_num,
0 as product_click_num,
case 
	when p_type = 'zy' then 4 
	else 32 
end as dim_value 
where center_id = 'jdzx' or center_id = 'kx' or center_id = 'fsxb' or center_id = 'jjjz' or center_id = 'qc' 
group by center_id,p_type,category_id_1,category_id_2,category_id_3 ;




