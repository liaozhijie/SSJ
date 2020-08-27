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
create table 
	temp.ssj_outer_ad_table_v1 
as select 
	ymd,group_name,position_id,system_name,sum(t.s_num) as show,sum(t.c_num) as click,sum(t.c_num)/sum(t.s_num) as ctr 
from 
    	(select 
		s.*,c.c_num 
	from 
        	(select ymd,request_id,group_name,position_id,system_name,count(*) as s_num from dw.bdl_bigdata_ad_outer_show_fodder where ymd >= default.calc_date(7) and ymd < default.calc_date(0) group by ymd,request_id,group_name,position_id,system_name) s
        left join
        	(select ymd,request_id, count(*) as c_num from dw.bdl_bigdata_ad_outer_click_fodder where ymd >= default.calc_date(7) and ymd < default.calc_date(0) group by ymd, request_id) c
        on 
            	s.ymd=c.ymd and s.request_id=c.request_id
    	) t
group by 
    	ymd,group_name,position_id,system_name
order by 
    	ymd,group_name,position_id,system_name;



drop table if exists temp.ssj_outer_ad_table_v2;
create table 
        temp.ssj_outer_ad_table_v2
as select
        ymd,group_name,position_id,system_name,sum(t.s_num) as show,sum(t.c_num) as click,sum(t.c_num)/sum(t.s_num) as ctr
from    
        (select 
                s.*,c.c_num 
        from
                (select ymd,request_id,group_name,position_id,system_name,count(*) as s_num from dw.bdl_bigdata_ad_outer_show_fodder where ymd >= default.calc_date(14) and ymd < default.calc_date(7) group by ymd,request_id,group_name,position_id,system_name) s
        left join
                (select ymd,request_id, count(*) as c_num from dw.bdl_bigdata_ad_outer_click_fodder where ymd >= default.calc_date(14) and ymd < default.calc_date(7) group by ymd, request_id) c
        on      
                s.ymd=c.ymd and s.request_id=c.request_id
        ) t     
group by 
        ymd,group_name,position_id,system_name
order by 
        ymd,group_name,position_id,system_name;



