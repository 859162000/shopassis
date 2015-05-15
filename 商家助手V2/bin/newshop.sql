
create temporary function getshopinfo as 'test.GetShopInfo';

insert overwrite table mtemp_shopwebdata_1
select a.uc,a.new_time,a.s,a.product_platform,a.v1,a.ru,a.i,a.ip,a.u,a.uid,a.log_day,a.log_hour,si.ptId,si.stId,si.shopId,si.productId,si.skuId
from default.sm_offline_datas a lateral view getshopinfo(ru,u,i) si as ptId,stId,shopId,productId,skuId  
where a.log_day = '20150512' and a.index = '1' and a.t = '0' and a.i in ('1','2') and length(a.u) = 16 and si.stId >= 1 and si.ptId >= 1;

insert overwrite table mtemp_shopwebdata_4
select uc,ptId,ru,stId,i,s,shopId,new_time from mtemp_shopwebdata_1 where shopId >= 1;

insert overwrite table mtemp_shopwebdata_4
select a.uc,a.ptId,a.ru,a.stId,a.i,a.s,b.shop_no shopId,a.new_time
from (select * from mtemp_shopwebdata_1 where shopId = 0) a join default.afcusdw_0_live_catb_0_gome_product_shop b
on a.productId = b.product_id;

from
(
select
	uc,
	ptId,
	ru,
	stId,
	i,
	s,
	shopId,
	new_time,
	unix_timestamp(new_time) times
from mtemp_shopwebdata_4
) tb1

insert overwrite table mtemp_shopwebdata_sesstion_5
select s,min(new_time) orgin_time,count(1) pv,IF(count(1) == 1,1,0) jumps,
(max(times) - min(times)) remains,
sum(IF(stId == 3003,1,0)) productinfo_pv,sum(IF(stId == 1,1,0)) cart_pv,sum(IF(stId == 2,1,0)) order_pv,sum(IF(stId == 3,1,0)) pay_pv
group by shopId;

insert overwrite table mtemp_shopwebdata_uv_7
select i,shopId,ptId,ru,count(distinct uc) uv 
group by i,shopId,ptId,ru;

from
(
select 
	b.i,
	b.shopId,
	b.s,
	b.ptId,
	b.ru,
	1 visits,
	1 landing_pv,
	a.pv,
	a.jumps,
	a.remains,
	a.productinfo_pv,
	a.cart_pv,
	a.order_pv,
	a.pay_pv 
from temp_shopwebdata_sesstion_5 a join (select * from temp_shopwebdata_4 where ptId != 3000) b 
on a.s = b.s and a.orgin_time = b.new_time;
)

insert overwrite table temp_shopwebdata_shopInfo_8
select i,shopId,ptId,ru,sum(landing_pv) landing_pv,sum(pv) pv,sum(productinfo_pv) productinfo_pv,
sum(cart_pv) cart_pv,sum(order_pv) order_pv,sum(pay_pv) pay_pv,sum(visits) visits,sum(jumps) jumps,sum(remains) remains,'' id 
group by i,shopId,ptId,ru;

insert overwrite table temp_shopwebdata_result_9
select a.shopId,'$datelong' visit_date,-1 visit_hour,i data_source
,a.ptId,a.ru,b.uv,a.landing_pv,a.pv,a.productinfo_pv,
a.cart_pv,a.order_pv,a.pay_pv,a.visits,a.jumps,a.remains,'' id
from temp_shopwebdata_shopInfo_8 a join temp_shopwebdata_uv_7 b
on a.i = b.i and a.shopId = b.shopId and a.ptId = b.ptId and a.ru = b.ru;




