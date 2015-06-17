
use ods_new;

drop table ods_new.sitemonitor_log;
CREATE TABLE ods_new.sitemonitor_log 
(
new_time string,
timestamp bigint,
hour int,
page_url string,
ref_url string,
user_cookie_id string,
uid string,
session_id string,
type_id string,
index string,
log_type string,
page_url_tag array<string>,
ref_url_tag array<string>,
channel_tag array<string>,
product_id string,
is_product int,
brand_id string,
center_id string,
category1_id string,
category2_id string,
category3_id string,
v1 string,
rv string,
is_real int,
is_exit int,
is_jump int,
lpg_type string,
last_access_time bigint,
ex_field map<string,string> 
)
partitioned by(dt int,type string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


drop table app_atg_order;
CREATE  TABLE app_atg_order
(
submitted_date string,
hour int,
order_id string, 
product_id string, 
amount float, 
quantity int, 
prd_brand string, 
center_id string,
category_id_1 string, 
category_id_2 string, 
category_id_3 string, 
user_id string, 
pay_success_flag int,
channel_tag array<string>,
lpg_type string
)
PARTITIONED BY (dt int,type string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;



drop table lpg_log;
CREATE TABLE lpg_log 
(
user_cookie_id string,
lpg string 
)
partitioned by(dt int,type string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


drop table center_relationship;
create table center_relationship 
(
center_id string,
center_name string,
c1_id string,
c1_name string,
c1_url string,
type string 
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


drop table mapping_product_categorg;
create table mapping_product_categorg 
(
p_id string,
c1_id string,
c2_id string,
c3_id string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;





DROP TABLE IF EXISTS dim_product_brand;
CREATE TABLE dim_product_brand 
(
p_id string,
p_brand string,
is_sale int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS dim_category;
CREATE TABLE dim_category 
(
category_id string,
category_name string,
depth int,
parent_id string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS dim_center;
CREATE TABLE dim_center (
center_id string,
center_name string,
category_1_name string,
category_1_url string,
category_1_id string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS dim_channel;
CREATE TABLE dim_channel (
channel_tag string,
channel_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS dim_conversion;
CREATE TABLE dim_conversion 
(
conversion_id int,
conversion_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS dim_conversion_step;
CREATE TABLE dim_conversion_step 
(
step_id int,
step_name string,
conversion_id int,
step_pattern_page_id int,
step_ref_pattern_page_id int,
sort_id int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS dim_paye_pattern;
CREATE TABLE dim_paye_pattern 
(
page_pattern string,
page_type string,
page_name string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS report_app_center_flow;
CREATE TABLE report_app_center_flow 
(
report_date int,
report_hour int,
center_id string,
dim1 string,
dim2 string,
uv int,
pv int,
exit_num int,
visits_num int,
sum_access_time bigint,
ipv int,
shop_cart_num int,
product_click_num int,
dim_value int
)
partitioned by(dt int,type string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS report_app_center_sale;
CREATE TABLE report_app_center_sale 
(
report_date int,
report_hour int,
center_id string,
dim1 string,
dim2 string,
dim3 string,
ipv int,
online_product_num int,
order_num int,
order_product_num int,
saled_product_num int,
sum_money float,
uv int,
cancel_order_num int,
product_click_num int,
dim_vale int
)
partitioned by(dt int,type string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS report_app_channel;
CREATE TABLE report_app_channel 
(
report_date int,
report_hour int,
channel string,
dim1 string,
dim2 string,
uv int,
pv int,
real_uv int,
order_num int,
sum_money float,
jump_num int,
dim_value int
)
partitioned by(dt int,type string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS report_app_conversion;
CREATE TABLE report_app_conversion 
(
report_date int,
report_hour int,
conversion_id int,
uv int,
real_uv int,
visits_num int
) 
partitioned by(dt int,type string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


DROP TABLE IF EXISTS report_app_date;
CREATE TABLE report_app_date 
(
report_date int,
report_hour int,
pv int,
uv int,
real_uv int,
shop_cart_uv int,
submit_uv int,
sum_money float,
order_num int
)
partitioned by(dt int,type string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;





DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product 
(
p_id string,
center_id string,
c1_id string,
c2_id string,
c3_id string,
is_sale int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;



