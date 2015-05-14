---ip位置
drop table ods_dmp.data_helper_area_number;
create table ods_dmp.data_helper_area_number as
select *,
if(ip RLIKE '[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$',split(ip,'\\.')[0]*256*256*256+split(ip,'\\.')[1]*256*256+split(ip,'\\.')[2]*256+split(ip,'\\.')[3],null) ipNumber
from ods_dmp.data_helper_newCooke;


drop table ods_dmp.data_helper_Integration_tmp;
create table ods_dmp.data_helper_Integration_tmp as
select tb1.*,
       if(tb1.ipNumber is null or tb2.province is null,null,tb2.province) province ,
       if(tb1.ipNumber is null or tb2.city is null,null,tb2.city)  city
from ods_dmp.data_helper_area_number tb1
left outer join ip_adderss_split tb2
on tb1.ipNumber = tb2.ipNum;

drop table ods_dmp.data_helper_Integration;
create table ods_dmp.data_helper_Integration as
select 
uc, 
i,                    
visit_ranking ,           
startrank,               
    endrank,                
    ranking,                
    time_difference,          
    u,                       
    ru,                       
    shop_id,                  
    ip,                       
    if(uid RLIKE '^[0-9]*$',uid,null) uid,                      
    if(new_time RLIKE '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}',new_time,null) new_time,                 
    jump_flag,                
    isnewcooke,               
    ipnumber,                 
    province,                 
    city,
    case
       when ru RLIKE '^http://www.gome.com.cn/\\?.*' or ru = 'http://www.gome.com.cn/'  then '国美首页'
       when ru RLIKE '^http://www.gome.com.cn/search\\?.*' then '站内搜索' 
       when ru RLIKE '^http://www.gome.com.cn/category.*'  then  '列表页'
       when ru RLIKE '^http://g.gome.com.cn/ec/homeus/cart/.*' or ru RLIKE '^http://g.gome.com.cn/ec/homeus/checkout/.*'  then '购物流程相关' 
       when ru RLIKE '^http://g.gome.com.cn/ec/homeus/.*'  and ru RLIKE 'myaccount' then '我的国美'
       when ru RLIKE '^http://tuan.gome.com.cn/.*' then '团购'
       when ru RLIKE '^http://www.gome.com.cn/ec/rushbuy/.*' then '抢购'
       when ru RLIKE '^http://fashion.gome.com.cn/\\?.*' or ru = 'http://fashion.gome.com.cn/' then '服装城'
       when ru RLIKE '^http://jipiao.gome.com.cn/.*' then '飞机票'
       when ru RLIKE '^http://caipiao.gome.com.cn/.*' then '彩票'
       when ru RLIKE '^http://chongzhi.gome.com.cn/.*' then '手机充值'
       when ru like 'http://jr.gome.com.cn/?cmpid=regular_shouye_myb%' or ru like 'http://g.gome.com.cn/ec/homeus/n/richTreasure/richImproveInfo.jsp%' then '美盈宝'
       when ru RLIKE '^http://game.gome.com.cn/.*' then '游戏'
       when ru like 'http://jiadian.gome.com.cn/%' then '家电城'
       when ru like 'http://market.gome.com.cn/%' then '国美超市'
       when ru like 'http://www.multigold.com.cn/%' then '多边黄金'
       when ru like 'http://enterprise.gome.com.cn/%' then '企业采购'
       when ru like 'http://www.gomeart.com/%' then '国之美'
       when ru like 'http://weixiu.gome.com.cn/%' then '家电维修'
       when ru like 'http://huishou.gome.com.cn/%' then '家电回收'
       when ru like 'http://cps.gome.com.cn/%' then '联盟销售'
       when ru like 'http://vip.gome.com.cn/%' then '唯品惠'
       when ru like 'http://www.gome.com.cn/bestGome.html%' then '最国美'
       when ru like 'http://www.gome.com.cn/sale.html%' then '大牌特惠'
       when ru like 'http://new.gome.com.cn%' then '新品首发'
       when ru like 'http://www.gomehome.com/%' then '国美家'
       when ru like 'http://v.gome.com.cn/%' then '会员俱乐部'
       when ru like 'http://prom.gome.com.cn/%' then '活动页面'
       when !(ru RLIKE 'gome') then '国美站外'
       else '其它'
    end as u_type 
from ods_dmp.data_helper_Integration_tmp;

