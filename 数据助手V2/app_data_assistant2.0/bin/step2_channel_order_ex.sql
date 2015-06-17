
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
channel_tag,
lpg_type 
from ods_new.app_atg_order 
where dt = {dt}{hour} and type = 'sm_order' 
) tbl 


--SEO
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'seo_word_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[3] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
case 
	when channel_tag[2] = 'brand' then 1 
	else 8 
end as dim_value 
where channel_tag[0] = 'seo' 
group by hour,channel_tag[0],channel_tag[2],channel_tag[3] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'seo_se_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
case 
	when channel_tag[2] = 'brand' then 2 
	else 16 
end as dim_value 
where channel_tag[0] = 'seo' 
group by hour,channel_tag[0],channel_tag[2],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'seo_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
case 
	when channel_tag[2] = 'brand' then 4 
	else 32 
end as dim_value 
where channel_tag[0] = 'seo' 
group by hour,channel_tag[0],channel_tag[2],lpg_type 



--SEM
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_pz_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[3] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
1 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'pinzhuan' 
group by hour,channel_tag[0],channel_tag[3] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_pz_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
0 as order_num,
sum(amount) as sum_money,
0 as jump_num,
2 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'pinzhuan' 
group by hour,channel_tag[0],lpg_type 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_pz_se_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
4 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'pinzhuan' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_word_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[3] as dim1,
channel_tag[4] as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
8 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'word' 
group by hour,channel_tag[0],channel_tag[3],channel_tag[4] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_word_se_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
16 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'word' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_word_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
16 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'word' 
group by hour,channel_tag[0],lpg_type 


--ad
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'ad_meida_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
1 as dim_value 
where channel_tag[0] = 'ad' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'ad_breakdown_meida_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[2] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
2 as dim_value 
where channel_tag[0] = 'ad' 
group by hour,channel_tag[0],channel_tag[2] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'ad_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
4 as dim_value 
where channel_tag[0] = 'ad' 
group by hour,channel_tag[0],lpg_type 

--BD
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'bd_breakdown_navigation_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
1 as dim_value 
where channel_tag[0] = 'bd' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'bd_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
2 as dim_value 
where channel_tag[0] = 'bd' 
group by hour,channel_tag[0],lpg_type 

--pro
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'pro_category_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
1 as dim_value 
where channel_tag[0] = 'pro' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'pro_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
2 as dim_value 
where channel_tag[0] = 'pro' 
group by hour,channel_tag[0],lpg_type 

--yj
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'yj_ru_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
1 as dim_value 
where channel_tag[0] = 'yj' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'yj_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
2 as dim_value 
where channel_tag[0] = 'yj' 
group by hour,channel_tag[0],lpg_type 

--dsp
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'dsp_breakdown_meida_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
1 as dim_value 
where channel_tag[0] = 'dsp' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'dsp_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
2 as dim_value 
where channel_tag[0] = 'dsp' 
group by hour,channel_tag[0],lpg_type 

--snm
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'snm_breakdown_meida_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
1 as dim_value 
where channel_tag[0] = 'snm' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'snm_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
2 as dim_value 
where channel_tag[0] = 'snm' 
group by hour,channel_tag[0],lpg_type 

--cps
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'cps_website_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
1 as dim_value 
where channel_tag[0] = 'cps' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'cps_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
2 as dim_value 
where channel_tag[0] = 'cps' 
group by hour,channel_tag[0],lpg_type 

--edm
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'edm_type_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
case 
	when channel_tag[1] = 'email' then 1 
	else 4 
end as dim_value 
where channel_tag[0] = 'edm' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'edm_trigger_mode_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[2] as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
case 
	when channel_tag[1] = 'email' then 16 
	else 32 
end as dim_value 
where channel_tag[0] = 'edm' 
group by hour,channel_tag[0],channel_tag[1],channel_tag[2] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'edm_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
case 
	when channel_tag[1] = 'email' then 2 
	else 8 
end as dim_value 
where channel_tag[0] = 'edm' 
group by hour,channel_tag[0],channel_tag[1],lpg_type


--direct
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'direct_lpg_order' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
0 as uv,
0 as pv,
0 as real_uv,
count(distinct order_id) as order_num,
sum(amount) as sum_money,
0 as jump_num,
1 as dim_value 
where channel_tag[0] = 'direct' 
group by hour,channel_tag[0],lpg_type


;
