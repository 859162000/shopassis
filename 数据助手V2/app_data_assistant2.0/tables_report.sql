
use ods_new;


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







