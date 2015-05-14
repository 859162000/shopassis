#!/bin/bash
cd /home/hadoop/data_helper_new/offine
datelong="$1"
datenow="$2"
/home/hadoop/hive/bin/hive -e "
drop table ods_dmp.date_helper_Integration_shop_sort;
create table ods_dmp.date_helper_Integration_shop_sort as
select uc,
       i,
       visit_ranking,
       ranking,
       time_difference,
       u,
       ru, 
       shop_id, 
       ip,
       uid,
       new_time,
       isnewcooke,
       province, 
       city,
       u_type,
       row_number() over(partition by uc,visit_ranking,shop_id,i order by ranking) behavior_ranking 
from ods_dmp.data_helper_Integration
where shop_id is not null;

drop table ods_dmp.date_helper_Integration_shop_single_statistics;
create table ods_dmp.date_helper_Integration_shop_single_statistics as
select shop_id SHOP_ID,
       uc COOKIE_ID,
       i,
       visit_ranking VISIT_ID,
       new_time INTO_SHOP_TIME,
       u_type INTO_SHOP_TYPE,
       ru INTO_SHOP_PAGE,
       u LANDING_PAGE,
       isnewcooke IS_NEW_USERVIEWS,
       uid USER_ID,
       province USER_POST_PROVINCE,
       city USER_POST_CITY
from ods_dmp.date_helper_Integration_shop_sort
where behavior_ranking = 1;

drop table ods_dmp.date_helper_Integration_shop_polymerization_statistics;
create table ods_dmp.date_helper_Integration_shop_polymerization_statistics as
select shop_id SHOP_ID,
       uc COOKIE_ID,
       i,
       visit_ranking VISIT_ID,
       count(*) SINGLE_VISIT_PAGE_NUMBER,
       sum(time_difference) SINGLE_STAY_TIME
from ods_dmp.date_helper_Integration_shop_sort
group by uc,visit_ranking,shop_id,i;


drop table ods_dmp.shop_offline_visit_statistics;
create table ods_dmp.shop_offline_visit_statistics as
select single.SHOP_ID,
       single.VISIT_ID,
       single.COOKIE_ID,
       single.INTO_SHOP_TIME,
       single.INTO_SHOP_TYPE,
       single.INTO_SHOP_PAGE,
       single.LANDING_PAGE,
       polymerization.SINGLE_VISIT_PAGE_NUMBER,
       polymerization.SINGLE_STAY_TIME,
       single.IS_NEW_USERVIEWS,
       single.USER_ID,
       single.USER_POST_PROVINCE,
       single.USER_POST_CITY,
       single.i
from ods_dmp.date_helper_Integration_shop_single_statistics single
inner join ods_dmp.date_helper_Integration_shop_polymerization_statistics polymerization
on single.COOKIE_ID = polymerization.COOKIE_ID 
and single.VISIT_ID = polymerization.VISIT_ID
and single.SHOP_ID = polymerization.SHOP_ID
and single.i = polymerization.i;

CREATE TEMPORARY FUNCTION converRu AS 'com.gome.udf.ConvertRu';

drop table ods_dmp.shop_offline_visit_statistics_import;
create table ods_dmp.shop_offline_visit_statistics_import as
select '' ID,
       SHOP_ID,
       VISIT_ID,
       COOKIE_ID,
       INTO_SHOP_TIME,
       INTO_SHOP_TYPE,
       if(INTO_SHOP_PAGE RLIKE '^http.+',converru(INTO_SHOP_PAGE),null) INTO_SHOP_PAGE,
       if(LANDING_PAGE RLIKE '^http.+',LANDING_PAGE,null) LANDING_PAGE,
       SINGLE_VISIT_PAGE_NUMBER,
       SINGLE_STAY_TIME,
       IS_NEW_USERVIEWS,
       USER_ID,
       USER_POST_PROVINCE,
       USER_POST_CITY,
       '$datelong' FDATE,
       i DATA_SOURCE,
       '$datenow' UPLOADTIME
from ods_dmp.shop_offline_visit_statistics;

"



/home/hadoop/sqoop-hive/bin/sqoop-export --connect jdbc:mysql://10.126.50.115:8306/bigdata  --username ae_prd --password Vja7hwuCH4Qrl4DI --table shop_offline_visit_statistics --export-dir /user/hive/warehouse/ods_dmp.db/shop_offline_visit_statistics_import  --input-null-string "\\\\N" --input-null-non-string "\\\\N" --input-fields-terminated-by "\\01" --input-lines-terminated-by "\\n";
