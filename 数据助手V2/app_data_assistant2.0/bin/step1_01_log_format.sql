
SET mapreduce.job.priority=HIGH;

add jar /home/hadoop/lsj/app_data_assistant2.0/lib/hive_udf.jar;

create temporary function getChannelTag as 'com.gome.hive.udf_new.getChannelTag';
create temporary function getUrlTag as 'com.gome.hive.udf_new.getUrlTag';
create temporary function getProductIdForUrl as 'com.gome.hive.udf.getProductIdForUrl';
create temporary function getCatIdForUrl as 'com.gome.hive.udf.getCatIdForUrl';
create temporary function getCenterIDForUrl as 'com.gome.hive.udf_new.getCenterIDForUrl';
create temporary function getCenterIDForCategory1ID as 'com.gome.hive.udf_new.getCenterIDForCategory1ID';

create temporary function gomeConcat as 'com.gome.hive.udaf.gomeConcat';
create temporary function getLogType as 'com.gome.hive.udf_new.getLogType';

use ods_new;


from 
(
select 
	new_time,
	unix_timestamp(new_time) as timestamp,
	hour(new_time) as hour,
	u as page_url,
	ru as ref_url,
	uc as user_cookie_id,
	uid,
	s as session_id,
	type_id,
	index,
	t as log_type,
	getUrlTag(u) as page_url_tag,
	getUrlTag(ru) as ref_url_tag,
	getChannelTag(cm) as channel_tag,
	case 
		when type_id = 54 then product_id 
		when type_id = 1 then product_id 
		else getProductIdForUrl(u) 
	end as product_id,
	case 
		when type_id = 54 then 1 
		when type_id = 1 then 1 
		when u like "http://www.gome.com.cn/product/%" then 1 
		when u like "http://item.gome.com.cn/%" then 1 
		else 0 
	end is_product,
	'' as brand_id,
	getCenterIDForUrl(u) as center_id,
	'' as category1_id,
	'' as category2_id,
	getCatIdForUrl(u) as category3_id,
	v1,
	rv,	
	count(1) over(partition by uc) as is_real,	
	row_number() over(partition by s order by unix_timestamp(new_time) desc) as is_exit,	
	count(1) over(partition by s) as is_jump,
	'' as lpg_type,
	sum(unix_timestamp(new_time)) over(partition by uc order by unix_timestamp(new_time) rows between 0 preceding and 1 following) as last_access_time,
	str_to_map(concat('orderid=',case when orderid is null then '' else orderid end,',orderprice=',case when orderprice is null then '' else orderprice end ,',ordertype=',case when ordertype is null then '' else ordertype end,',productid=',case when product_id is null then '' else productid end ,',product_price=',case when product_price is null then '' else product_price end ,',product_num=',case when product_num is null then '' else product_num end,',','=')) as ex_field 
from default.sm_offline_datas 
where log_day = {dt} and log_hour >= {dt}00 and log_hour <= {dt}{hour} and new_time is not null and u is not null and length(uc) = 16 and t is not null and i = '1' 
) tbl 

INSERT OVERWRITE TABLE ods_new.sitemonitor_log partition ( dt = {dt}{hour} , type = 'step1_product' ) 
select 
new_time,
timestamp,
hour,
page_url,
ref_url,
user_cookie_id,
uid,
session_id,
type_id,
index,
log_type,
page_url_tag,
ref_url_tag,
channel_tag,
product_id,
is_product,
brand_id,
center_id,
category1_id,
category2_id,
category3_id,
v1,
rv,
case 
	when is_real = 1 then 0 
	else 1 
end as is_real,
case 
	when is_exit = 1 then 1
	else 0 
end as is_exit,
case 
	when is_jump = 1 then 1 
	else 0 
end as is_jump,
lpg_type,
last_access_time - timestamp - timestamp as last_access_time,
ex_field 
where is_product = 1 

INSERT OVERWRITE TABLE ods_new.sitemonitor_log partition ( dt = {dt}{hour} , type = 'step1_noproduct' ) 
select 
new_time,
timestamp,
hour,
page_url,
ref_url,
user_cookie_id,
uid,
session_id,
type_id,
index,
log_type,
page_url_tag,
ref_url_tag,
channel_tag,
product_id,
is_product,
brand_id,
center_id,
category1_id,
category2_id,
category3_id,
v1,
rv,
case 
	when is_real = 1 then 0 
	else 1 
end as is_real,
case 
	when is_exit =1 then 1 
	else 0 
end as is_exit,
case 
	when is_jump = 1 then 1 
	else 0 
end as is_jump,
lpg_type,
--mikai 20150515 edit
--timestamp + timestamp - last_access_time as last_access_time,
last_access_time - timestamp - timestamp as last_access_time,
ex_field 
where is_product = 0;



from 
(
select 
logs.new_time,
logs.timestamp,
logs.hour,
logs.page_url,
logs.ref_url,
logs.user_cookie_id,
logs.uid,
logs.session_id,
logs.type_id,
logs.index,
logs.log_type,
logs.page_url_tag,
logs.ref_url_tag,
logs.channel_tag,
logs.product_id,
logs.is_product,
logs.brand_id,
case 
	when c.c1_id is null then logs.center_id 
	else getCenterIDForCategory1ID(c.c1_id) 
end as center_id,
c.c1_id as category1_id,
c.c2_id as category2_id,
c.c3_id as category3_id,
logs.v1,
logs.rv,
logs.is_real,
logs.is_exit,
logs.is_jump,
logs.lpg_type,
logs.last_access_time,
logs.ex_field 
from ods_new.mapping_product_categorg c 
right outer join ods_new.sitemonitor_log logs 
on logs.product_id =  c.p_id 
where logs.dt = {dt}{hour} and logs.type = 'step1_product' 
) tbl 

INSERT OVERWRITE TABLE ods_new.sitemonitor_log partition ( dt = {dt}{hour} , type = 'step2_mapping_product_category' ) 
select 
new_time,
timestamp,
hour,
page_url,
ref_url,
user_cookie_id,
uid,
session_id,
type_id,
index,
log_type,
page_url_tag,
ref_url_tag,
channel_tag,
product_id,
is_product,
brand_id,
center_id,
category1_id,
category2_id,
category3_id,
v1,
rv,
is_real,
is_exit,
is_jump,
lpg_type,
last_access_time,
ex_field ;

INSERT OVERWRITE TABLE ods_new.lpg_log partition ( dt = {dt}{hour} , type = 'step1_origin' ) 
select 
user_cookie_id,
gomeConcat(concat(page_url_tag[2],'-',timestamp),',') as lpg 
from ods_new.sitemonitor_log 
where dt = {dt}{hour} and (type = 'step2_mapping_product_category' or type = 'step1_noproduct') and ref_url_tag[3] = 'external' and log_type = 0 and index = 1 
group by user_cookie_id ;


INSERT OVERWRITE TABLE ods_new.sitemonitor_log partition ( dt = {dt}{hour} , type = 'step3_done' ) 
select 
logs.new_time,
logs.timestamp,
logs.hour,
logs.page_url,
logs.ref_url,
logs.user_cookie_id,
logs.uid,
logs.session_id,
logs.type_id,
logs.index,
logs.log_type,
logs.page_url_tag,
logs.ref_url_tag,
logs.channel_tag,
logs.product_id,
logs.is_product,
logs.brand_id,
logs.center_id,
logs.category1_id,
logs.category2_id,
logs.category3_id,
logs.v1,
logs.rv,
logs.is_real,
logs.is_exit,
logs.is_jump,
case 
	when lpg.user_cookie_id is not null then getLogType(lpg.lpg,logs.timestamp) 
	else 'nona' 
end as lpg_type,
logs.last_access_time,
logs.ex_field 
from ods_new.lpg_log lpg 
right outer join ods_new.sitemonitor_log logs 
on logs.user_cookie_id = lpg.user_cookie_id 
where logs.dt = {dt}{hour} and (logs.type = 'step2_mapping_product_category' or logs.type = 'step1_noproduct') and lpg.dt = {dt}{hour} and lpg.type = 'step1_origin';

