
SET mapreduce.job.priority=HIGH;

use ods_new;

insert overwrite table ods_new.report_app_center_sale partition ( dt = {dt}{hour} , type = 'all' ) 
select 
tbl1.report_date,
tbl1.report_hour,
tbl1.center_id,
tbl1.dim1,
tbl1.dim2,
tbl1.dim3,
sum(tbl1.ipv),
sum(tbl2.online_product_num),
sum(tbl1.order_num),
sum(tbl1.order_product_num),
sum(tbl1.saled_product_num),
sum(tbl1.sum_money),
sum(tbl1.uv),
sum(tbl1.cancel_order_num),
sum(tbl1.product_click_num),
tbl1.dim_vale 
from 
(
select 
report_date,
report_hour,
center_id,
dim1,
dim2,
dim3,
ipv,
online_product_num,
order_num,
order_product_num,
saled_product_num,
sum_money,
uv,
cancel_order_num,
product_click_num,
dim_vale 
from ods_new.report_app_center_sale 
where dt = {dt}{hour} and type <> 'all' and type <> 'center_c1_op' and type <> 'center_c1_c2_op' and type <> 'center_c1_c2_c3_op' 
) tbl1 
left outer join 
(
select 
center_id,
dim1,
dim2,
dim3,
dim_vale,
online_product_num 
from ods_new.report_app_center_sale 
where dt = {dt}{hour} and type = 'center_c1_op' and type = 'center_c1_c2_op' and type = 'center_c1_c2_c3_op' 
) tbl2 
on tbl1.center_id = tbl2.center_id and tbl1.dim1 = tbl2.dim1 and tbl1.dim2 = tbl2.dim2 and tbl1.dim3 = tbl2.dim3 and tbl1.dim_vale = tbl2.dim_vale 
group by tbl1.report_date,tbl1.report_hour,tbl1.center_id,tbl1.dim1,tbl1.dim2,tbl1.dim3,tbl1.dim_vale;

