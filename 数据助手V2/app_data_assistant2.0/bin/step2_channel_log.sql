
SET mapreduce.job.priority=HIGH;

use ods_new;

from 
(
select 
hour,
channel_tag,
page_url_tag,
user_cookie_id,
is_real,
is_jump,
lpg_type 
from ods_new.sitemonitor_log 
where dt = {dt}{hour} and type = 'step3_done' and log_type = '0' and index = '1' 
) tbl 


--SEO
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'seo_word_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[3] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
case 
	when channel_tag[2] = 'brand' then 1 
	else 8 
end as dim_value 
where channel_tag[0] = 'seo' 
group by hour,channel_tag[0],channel_tag[2],channel_tag[3] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'seo_se_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
case 
	when channel_tag[2] = 'brand' then 2 
	else 16 
end as dim_value 
where channel_tag[0] = 'seo' 
group by hour,channel_tag[0],channel_tag[2],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'seo_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
case 
	when channel_tag[2] = 'brand' then 4 
	else 32 
end as dim_value 
where channel_tag[0] = 'seo' 
group by hour,channel_tag[0],channel_tag[2],lpg_type 



--SEM
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_pz_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[3] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
1 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'pinzhuan' 
group by hour,channel_tag[0],channel_tag[3] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_pz_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
2 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'pinzhuan' 
group by hour,channel_tag[0],lpg_type 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_pz_se_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
4 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'pinzhuan' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_word_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[3] as dim1,
channel_tag[4] as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
8 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'word' 
group by hour,channel_tag[0],channel_tag[3],channel_tag[4] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_word_se_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
16 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'word' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'sem_word_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
16 as dim_value 
where channel_tag[0] = 'sem' and channel_tag[2] = 'word' 
group by hour,channel_tag[0],lpg_type 


--ad
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'ad_meida_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
1 as dim_value 
where channel_tag[0] = 'ad' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'ad_breakdown_meida_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[2] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
2 as dim_value 
where channel_tag[0] = 'ad' 
group by hour,channel_tag[0],channel_tag[2] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'ad_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
4 as dim_value 
where channel_tag[0] = 'ad' 
group by hour,channel_tag[0],lpg_type 

--BD
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'bd_breakdown_navigation_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
1 as dim_value 
where channel_tag[0] = 'bd' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'bd_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
2 as dim_value 
where channel_tag[0] = 'bd' 
group by hour,channel_tag[0],lpg_type 

--pro
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'pro_category_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
1 as dim_value 
where channel_tag[0] = 'pro' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'pro_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
2 as dim_value 
where channel_tag[0] = 'pro' 
group by hour,channel_tag[0],lpg_type 

--yj
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'yj_ru_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
1 as dim_value 
where channel_tag[0] = 'yj' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'yj_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
2 as dim_value 
where channel_tag[0] = 'yj' 
group by hour,channel_tag[0],lpg_type 

--dsp
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'dsp_breakdown_meida_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
1 as dim_value 
where channel_tag[0] = 'dsp' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'dsp_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
2 as dim_value 
where channel_tag[0] = 'dsp' 
group by hour,channel_tag[0],lpg_type 

--snm
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'snm_breakdown_meida_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
1 as dim_value 
where channel_tag[0] = 'snm' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'snm_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
2 as dim_value 
where channel_tag[0] = 'snm' 
group by hour,channel_tag[0],lpg_type 

--cps
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'cps_website_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
1 as dim_value 
where channel_tag[0] = 'cps' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'cps_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
2 as dim_value 
where channel_tag[0] = 'cps' 
group by hour,channel_tag[0],lpg_type 

--edm
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'edm_type_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[1] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
case 
	when channel_tag[1] = 'email' then 1 
	else 4 
end as dim_value 
where channel_tag[0] = 'edm' 
group by hour,channel_tag[0],channel_tag[1] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'edm_trigger_mode_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
channel_tag[2] as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
case 
	when channel_tag[1] = 'email' then 16 
	else 32 
end as dim_value 
where channel_tag[0] = 'edm' 
group by hour,channel_tag[0],channel_tag[1],channel_tag[2] 

insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'edm_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
case 
	when channel_tag[1] = 'email' then 2 
	else 8 
end as dim_value 
where channel_tag[0] = 'edm' 
group by hour,channel_tag[0],channel_tag[1],lpg_type


--direct
insert overwrite table ods_new.report_app_channel partition ( dt = {dt}{hour} , type = 'direct_lpg_log' ) 
select  
{dt} as report_date,
hour as report_hour,
channel_tag[0] as channel,
lpg_type as dim1,
'' as dim2,
count(distinct user_cookie_id) as uv,
count(1) as pv,
count(distinct case when is_real = 1 then user_cookie_id else '' end) as real_uv,
0 as order_num,
0 as sum_money,
sum(is_jump) as jump_num,
1 as dim_value 
where channel_tag[0] = 'direct' 
group by hour,channel_tag[0],lpg_type


;
