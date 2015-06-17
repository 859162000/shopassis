
use ods_new;


drop table ods_new.center_relationship;
create table ods_new.center_relationship 
(
center_id string,
center_name string,
c1_id string,
c1_name string,
c1_url string,
type string 
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ;


drop table ods_new.mapping_product_categorg;
create table ods_new.mapping_product_categorg 
(
p_id string,
c1_id string,
c2_id string,
c3_id string
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








