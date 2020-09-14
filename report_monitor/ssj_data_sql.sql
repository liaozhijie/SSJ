drop table if exists temp.ssj_busitype_table;
create table 
	temp.ssj_busitype_table 
as SELECT
    	ymd as ymd,'ssj' as product,positionid as positionid,systemname as systemname,busitype as busitype,account_name as account_name,groupname,count(distinct udid) as uv, sum(s_num) as show,sum(c_num) as click,float(sum(c_num)/sum(s_num)) as ctr
FROM
	(SELECT
    		v.*,s.*,c.*,ac.account_name
	FROM
    		(SELECT 
			ymd,requestid,count(*) as s_num FROM dw.bdl_bd_online_ad_client_show_log  WHERE ymd >= default.calc_date(67) and ymd < default.calc_date(0)  AND lower(productname) like '%mymoney%' group by requestid,ymd) s
		LEFT JOIN
    			(select ymd as c_ymd,requestid as c_requestid,count(*) as c_num FROM dw.bdl_bd_onlinead_clicklog where ymd >= default.calc_date(67) and ymd < default.calc_date(0) and lower(productname) like '%mymoney%'  group by ymd, requestid) c
		ON
    			(s.ymd = c.c_ymd and s.requestid = c.c_requestid)
		LEFT JOIN
    			(SELECT requestid as v_requestid,systemname,udid,busitype,positionid,groupname FROM dw.bdl_bd_onlinead_viewlog WHERE ymd >= default.calc_date(75) AND lower(productname)  like '%mymoney%')  v
		ON
    			(v.v_requestid = s.requestid)
		LEFT JOIN
    			dw.bdl_sms_b_account ac
		ON
    			v.busitype = ac.id
		) tt
where 
	account_name in ('理财-随手记','生活服务-随手记','办卡-随手记','产品功能-随手记','账本市场-随手记','保险-随手记','产品运营-随手记')
group by
    	ymd,positionid,systemname,busitype, account_name,groupname
having 
    	sum(s_num) > 500 and sum(c_num) > 0
order by
    	ymd,positionid,systemname,busitype, account_name, groupname;



drop table if exists temp.ssj_outer_ad_table_v1;
create table temp.ssj_outer_ad_table_v1 as
select a.ymd as ymd,  ---日期
systemname as system_name, ---系统
d.position_name as position_id, ---运营位
adrequest as adrequest,  ---请求量
adrequest_cnt as adrequest_cnt,  ---请求设备数
adback as adback,---下发量
adback_cnt as adback_cnt,---下发设备数
YLH_adview as YLH_adview , ---曝光量
CSJ_adview as CSJ_adview , ---曝光量
adview_cnt as adview_cnt, ---曝光设备数
YLH_adclick as YLH_adclick, ---点击量
CSJ_adclick as CSJ_adclick, ---点击量
adclick_cnt as adclick_cnt, ---点击设备数
adclose as adclose, ---关闭量
adclose_cnt as adclose_cnt, ---关闭设备数
adback/adrequest as Filling_rate, ---填充率
YLH_adclick/YLH_adview  as YLH_adclick_rate,  ---点击率
CSJ_adclick/CSJ_adview  as CSJ_adclick_rate,  ---点击率
adclick_cnt/adview_cnt  as adclick_cnt_rate,
adclose_cnt/adview_cnt  as adclose_cnt_rate
from (
select ymd,position,case when systemname <> 'android OS' then 'ios' else 'Android' end as systemname,
count(case when aoperation in ('优量汇_开始请求接口','优量汇_闪屏_开始请求接口','穿山甲_开始请求接口','穿山甲_闪屏_开始请求接口','闪屏_穿山甲_开始请求接口') then udid end) as adrequest,
count(distinct case when aoperation in ('优量汇_开始请求接口','优量汇_闪屏_开始请求接口','穿山甲_开始请求接口','穿山甲_闪屏_开始请求接口','闪屏_穿山甲_开始请求接口') then udid end) as adrequest_cnt,
count(case when aoperation in ('优量汇_开始渲染','优量汇_闪屏_开始下载素材','穿山甲_开始渲染','穿山甲_闪屏_开始下载素材','闪屏_穿山甲_开始下载素材') then udid end) as adback,
count(distinct case when aoperation in ('优量汇_开始渲染','优量汇_闪屏_开始下载素材','穿山甲_开始渲染','穿山甲_闪屏_开始下载素材','闪屏_穿山甲_开始下载素材') then udid end) as adback_cnt,
count(case when (aoperation like '%优量汇%广告浏览%') then udid end) as YLH_adview,
count(case when (aoperation like '%穿山甲%广告浏览%') then udid end) as CSJ_adview,
count(distinct case when (aoperation like '%优量汇%广告浏览%' or aoperation like '%穿山甲%广告浏览%') then udid end) as adview_cnt,
count(case when (aoperation like '%优量汇%点击广告内容%') then udid end) as YLH_adclick,
count(case when (aoperation like '%穿山甲%点击广告内容%') then udid end) as CSJ_adclick,
count(distinct case when (aoperation like '%优量汇%点击广告内容%' or aoperation like '%穿山甲%点击广告内容%') then udid end) as adclick_cnt,
count(case when aoperation in ('优量汇_广告选择关闭原因','穿山甲_广告选择关闭原因') then udid end) as adclose,
count(distinct case when aoperation in ('优量汇_广告选择关闭原因','穿山甲_广告选择关闭原因') then udid end) as adclose_cnt
from
(select if(aoperation like '%闪屏%','SP',get_json_object(custom1,'$.position')) as position,ymd,systemname,udid,aoperation 
 from dw.bdl_ssj_operations 
 where ymd >= default.calc_date(7) and ymd < default.calc_date(0)
 and (aoperation like '%优量汇%'  or aoperation like '%穿山甲%' )
)a
 where default.is_notempty(position)
 group by ymd,position,case when systemname <> 'android OS' then 'ios' else 'Android' end)a
 left join
(select distinct position_code,position_name from dw.bdl_sms_b_position) d on a.position = d.position_code;



drop table if exists temp.ssj_outer_ad_table_v2;
create table temp.ssj_outer_ad_table_v2 as
select a.ymd as ymd,  ---日期
systemname as system_name, ---系统
d.position_name as position_id, ---运营位
adrequest as adrequest,  ---请求量
adrequest_cnt as adrequest_cnt,  ---请求设备数
adback as adback,---下发量
adback_cnt as adback_cnt,---下发设备数
YLH_adview as YLH_adview , ---曝光量
CSJ_adview as CSJ_adview , ---曝光量
adview_cnt as adview_cnt, ---曝光设备数
YLH_adclick as YLH_adclick, ---点击量
CSJ_adclick as CSJ_adclick, ---点击量
adclick_cnt as adclick_cnt, ---点击设备数
adclose as adclose, ---关闭量
adclose_cnt as adclose_cnt, ---关闭设备数
adback/adrequest as Filling_rate, ---填充率
YLH_adclick/YLH_adview  as YLH_adclick_rate,  ---点击率
CSJ_adclick/CSJ_adview  as CSJ_adclick_rate,  ---点击率
adclick_cnt/adview_cnt  as adclick_cnt_rate,
adclose_cnt/adview_cnt  as adclose_cnt_rate
from (
select ymd,position,case when systemname <> 'android OS' then 'ios' else 'Android' end as systemname,
count(case when aoperation in ('优量汇_开始请求接口','优量汇_闪屏_开始请求接口','穿山甲_开始请求接口','穿山甲_闪屏_开始请求接口','闪屏_穿山甲_开始请求接口') then udid end) as adrequest,
count(distinct case when aoperation in ('优量汇_开始请求接口','优量汇_闪屏_开始请求接口','穿山甲_开始请求接口','穿山甲_闪屏_开始请求接口','闪屏_穿山甲_开始请求接口') then udid end) as adrequest_cnt,
count(case when aoperation in ('优量汇_开始渲染','优量汇_闪屏_开始下载素材','穿山甲_开始渲染','穿山甲_闪屏_开始下载素材','闪屏_穿山甲_开始下载素材') then udid end) as adback,
count(distinct case when aoperation in ('优量汇_开始渲染','优量汇_闪屏_开始下载素材','穿山甲_开始渲染','穿山甲_闪屏_开始下载素材','闪屏_穿山甲_开始下载素材') then udid end) as adback_cnt,
count(case when (aoperation like '%优量汇%广告浏览%') then udid end) as YLH_adview,
count(case when (aoperation like '%穿山甲%广告浏览%') then udid end) as CSJ_adview,
count(distinct case when (aoperation like '%优量汇%广告浏览%' or aoperation like '%穿山甲%广告浏览%') then udid end) as adview_cnt,
count(case when (aoperation like '%优量汇%点击广告内容%') then udid end) as YLH_adclick,
count(case when (aoperation like '%穿山甲%点击广告内容%') then udid end) as CSJ_adclick,
count(distinct case when (aoperation like '%优量汇%点击广告内容%' or aoperation like '%穿山甲%点击广告内容%') then udid end) as adclick_cnt,
count(case when aoperation in ('优量汇_广告选择关闭原因','穿山甲_广告选择关闭原因') then udid end) as adclose,
count(distinct case when aoperation in ('优量汇_广告选择关闭原因','穿山甲_广告选择关闭原因') then udid end) as adclose_cnt
from
(select if(aoperation like '%闪屏%','SP',get_json_object(custom1,'$.position')) as position,ymd,systemname,udid,aoperation
 from dw.bdl_ssj_operations
 where ymd >= default.calc_date(14) and ymd < default.calc_date(7)
 and (aoperation like '%优量汇%'  or aoperation like '%穿山甲%' )
)a
 where default.is_notempty(position)
 group by ymd,position,case when systemname <> 'android OS' then 'ios' else 'Android' end)a
 left join
(select distinct position_code,position_name from dw.bdl_sms_b_position) d on a.position = d.position_code;



drop table if exists temp.ssj_distinct_origid_count;
create table 
	temp.ssj_distinct_origid_count 
as select 
	t.*,ac.account_name 
from 
	(select positionid,systemname,busitype,count(distinct origid) as origid_count from dw.bdl_bd_onlinead_viewlog where ymd>=default.calc_date(7) and lower(productname) like '%mymoney%' and positionid in ('SP','SSJSYYYW') group by positionid,systemname,busitype) t
left join 
	dw.bdl_sms_b_account ac 
on 
	t.busitype=ac.id;



drop table if exists temp.ssj_ad_ecpm;
create table temp.ssj_ad_ecpm
as select t.*,ac.account_name from
(select a.positionid as positionid,a.systemname as systemname,a.groupname,b.busitype,a.e_expose as expose,b.clicks as click,b.income as income,1000*b.income/a.e_expose as ecpm , b.clicks/a.e_expose as ctr,b.income /b.clicks as bids,b.income_2,b.max_bid
from
(select view.positionid,view.systemname,view.busitype,view.groupname,sum(show.s_num) as e_expose from
        (SELECT requestid,count(*) as s_num from dw.bdl_bd_online_ad_client_show_log WHERE ymd>=default.calc_date(7) and ymd < default.calc_date(0)  AND (lower(productname) like '%mymoney%') and positionid in ('SP','SSJSYYYW') group by requestid) show
        left join
        (select requestid as v_requestid,positionid,systemname,groupname,busitype FROM dw.bdl_bd_onlinead_viewlog WHERE ymd>=default.calc_date(10) AND (lower(productname) like '%mymoney%')) view
        on show.requestid=view.v_requestid
    group by positionid,systemname,busitype,groupname
) as a
join
(
SELECT v.systemname,v.busitype,v.positionid,v.groupname,count(1) as clicks,sum(0.0001*v.bid) as income,sum(0.0001*v.bid*v.coeff) as income_2,max(0.0001*v.bid*v.coeff) as max_bid
FROM
    (SELECT requestid,count(*) from dw.bdl_bd_onlinead_clicklog  WHERE ymd>=default.calc_date(7) and ymd < default.calc_date(0)  AND (lower(productname) like '%mymoney%') and positionid in ('SP','SSJSYYYW') group by requestid) as s
    left JOIN
    (SELECT requestid as v_requestid,positionid,systemname,busitype,groupname,bid,coeff FROM dw.bdl_bd_onlinead_viewlog WHERE ymd>=default.calc_date(10) AND (lower(productname) like '%mymoney%') group by  requestid,ymd,positionid,systemname,busitype,groupname,bid,coeff ) as v
    ON s.requestid = v.v_requestid
group by positionid,systemname,groupname,busitype
)as b
on  a.systemname = b.systemname and a.busitype=b.busitype
and a.positionid = b.positionid and a.groupname=b.groupname)t
left join
dw.bdl_sms_b_account ac
on t.busitype = ac.id;











