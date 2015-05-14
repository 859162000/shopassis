#!/bin/bash
cd /home/hadoop/data_helper_new/offine
datestart="$3"
dateend="$4"
/home/hadoop/hive/bin/hive -e "
drop table ods_dmp.data_helper_c_rank;
create table ods_dmp.data_helper_c_rank as
select uc,
       i,
       shop_id,
       u,
       ru,
       ip,
       uid,
       new_time,
       row_number() over(partition by uc,i order by new_time) ranking 
from ods_dmp.date_helper_analyShop;

--算停留时间
drop table ods_dmp.data_helper_adjacentTime;
create table ods_dmp.data_helper_adjacentTime as
select tmp1.*,unix_timestamp(tmp2.new_time)-unix_timestamp(tmp1.new_time) time_difference from ods_dmp.data_helper_c_rank tmp1
left outer join ods_dmp.data_helper_c_rank tmp2
on tmp1.ranking+1=tmp2.ranking and tmp1.uc=tmp2.uc and tmp1.i=tmp2.i;

drop table ods_dmp.data_helper_stayTime_adjacentTime;
create table ods_dmp.data_helper_stayTime_adjacentTime as
select uc,i,shop_id,u,ru,ip,uid,new_time,ranking,if(time_difference is not null,time_difference,0) time_difference 
from ods_dmp.data_helper_adjacentTime;


--访问次数
drop table ods_dmp.data_helper_visit_adjacentTime;
create table ods_dmp.data_helper_visit_adjacentTime as
select * from (
select *,case 
            when time_difference is null then 1
            when time_difference >=1800 then 1
            when time_difference <1800 then 0
         end as visit_type
from ods_dmp.data_helper_adjacentTime
) tb where visit_type = 1;

drop table ods_dmp.data_helper_visit_adjacentTime_rank;
create table ods_dmp.data_helper_visit_adjacentTime_rank as
select uc,i,time_difference,new_time,ranking,row_number() over(partition by uc,i order by ranking) visit_ranking
from ods_dmp.data_helper_visit_adjacentTime;

drop table ods_dmp.data_helper_visit_adjacentTime_rank_connect;
create table ods_dmp.data_helper_visit_adjacentTime_rank_connect as
select tmp1.uc,tmp1.i,tmp1.visit_ranking,if(tmp2.ranking is not null,tmp2.ranking+1,1) startRank,tmp1.ranking endRank
from ods_dmp.data_helper_visit_adjacentTime_rank tmp1
left outer join ods_dmp.data_helper_visit_adjacentTime_rank tmp2
on tmp1.uc=tmp2.uc and tmp1.i=tmp2.i and tmp1.visit_ranking-1 = tmp2.visit_ranking;

drop table ods_dmp.data_helper_visit;
create table ods_dmp.data_helper_visit as
select * from (
select t1.uc,t1.i,t1.visit_ranking,t1.startRank,t1.endRank,t2.ranking,t2.time_difference,t2.u,t2.ru,t2.shop_id,t2.ip,t2.uid,t2.new_time from ods_dmp.data_helper_visit_adjacentTime_rank_connect t1
left outer join ods_dmp.data_helper_stayTime_adjacentTime t2
on t1.uc = t2.uc and t1.i=t2.i
) t where ranking between startRank and endRank;

--算跳出
drop table ods_dmp.data_helper_jump_tmp;
create table ods_dmp.data_helper_jump_tmp as
select uc,i,visit_ranking,count(*) times from ods_dmp.data_helper_visit
group by uc,i,visit_ranking having times=1;


drop table ods_dmp.data_helper_jump;
create table ods_dmp.data_helper_jump as
select t1.*,if(t2.uc is not null,1,0) jump_flag from ods_dmp.data_helper_visit t1
left outer join ods_dmp.data_helper_jump_tmp t2
on t1.uc = t2.uc and t1.i=t2.i and t1.visit_ranking = t2.visit_ranking;



--算新老客户

drop table ods_dmp.date_helper_old_main;
create table ods_dmp.date_helper_old_main as 
select * from (
select uc,i,u,ru,ip,new_time,log_day,log_hour from default.sm_offline_datas
where index = '1' and t = '0'
) tb where  log_day between '$datestart' and '$dateend';


drop table ods_dmp.data_helper_newCooke;
create table ods_dmp.data_helper_newCooke as
select tb1.*,if(tb2.uc is not null,0,1) isNewCooke from ods_dmp.data_helper_jump tb1
left outer join (
select uc,i from ods_dmp.date_helper_old_main
group by uc,i
) tb2 on tb1.uc = tb2.uc and tb1.i = tb2.i;
"

