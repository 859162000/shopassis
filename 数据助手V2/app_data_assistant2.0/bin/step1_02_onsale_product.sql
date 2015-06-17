
SET mapreduce.job.priority=HIGH;

add jar /home/hadoop/lsj/app_data_assistant2.0/lib/hive_udf.jar;
create temporary function getCenterIDForCategory1ID as 'com.gome.hive.udf_new.getCenterIDForCategory1ID';

use ods_new;

INSERT OVERWRITE TABLE ods_new.dim_product 
select 
m.p_id,
getCenterIDForCategory1ID(m.c1_id) as center_id,
m.c1_id,
m.c2_id,
m.c3_id,
d.is_sale 
from mapping_product_categorg m 
join dim_product_brand d 
on m.p_id = d.p_id ;

