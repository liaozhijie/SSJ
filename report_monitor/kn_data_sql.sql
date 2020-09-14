drop table if exists temp.kn_busitype_table;
create table 
	temp.kn_busitype_table 
as SELECT
    	ymd as ymd,'kn' as product,positionid as positionid,systemname as systemname,busitype as busitype,account_name as account_name,groupname,count(distinct udid) as uv, sum(s_num) as show,sum(c_num) as click,float(sum(c_num)/sum(s_num)) as ctr
FROM
	(SELECT
    		v.*,s.*,c.*,ac.account_name
	FROM
    		(SELECT 
			ymd,requestid,count(*) as s_num FROM dw.bdl_bd_online_ad_client_show_log  WHERE ymd >= default.calc_date(67) and ymd < default.calc_date(0)  AND lower(productname) like '%card%' group by requestid,ymd) s
		LEFT JOIN
    			(select ymd as c_ymd,requestid as c_requestid,count(*) as c_num FROM dw.bdl_bd_onlinead_clicklog where ymd >= default.calc_date(67) and ymd < default.calc_date(0) and lower(productname) like '%card%'  group by ymd, requestid) c
		ON
    			(s.ymd = c.c_ymd and s.requestid = c.c_requestid)
		LEFT JOIN
    			(SELECT requestid as v_requestid,systemname,udid,busitype,positionid,groupname FROM dw.bdl_bd_onlinead_viewlog WHERE ymd >= default.calc_date(75) AND lower(productname)  like '%card%')  v
		ON
    			(v.v_requestid = s.requestid)
		LEFT JOIN
    			dw.bdl_sms_b_account ac
		ON
    			v.busitype = ac.id
		) tt
group by
    	ymd,positionid,systemname,busitype, account_name,groupname
having 
    	sum(s_num) > 500 and sum(c_num) > 0
order by
    	ymd,positionid,systemname,busitype, account_name, groupname;



drop table if exists temp.kn_distinct_origid_count;
create table
        temp.kn_distinct_origid_count
as select
        t.*,ac.account_name
from
        (select positionid,systemname,busitype,count(distinct origid) as origid_count from dw.bdl_bd_onlinead_viewlog where ymd>=default.calc_date(7) and lower(productname) like '%card%' and positionid in ('SP','KNSYFW') group by positionid,systemname,busitype) t
left join
        dw.bdl_sms_b_account ac
on
        t.busitype=ac.id;



drop table if exists temp.kn_ad_ecpm;
create table temp.kn_ad_ecpm
as select t.*,ac.account_name from
(select a.positionid as positionid,a.systemname as systemname,a.groupname,b.busitype,a.e_expose as expose,b.clicks as click,b.income as income,1000*b.income/a.e_expose as ecpm , b.clicks/a.e_expose as ctr,b.income /b.clicks as bids ,b.income_2,b.max_bid
from
(select view.positionid,view.systemname,view.busitype,view.groupname,sum(show.s_num) as e_expose from
        (SELECT requestid,count(*) as s_num from dw.bdl_bd_online_ad_client_show_log WHERE ymd>=default.calc_date(7) and ymd < default.calc_date(0)  AND (lower(productname) like '%card%') and positionid in ('SP','KNSYFW') group by requestid) show
        left join
        (select requestid as v_requestid,positionid,systemname,groupname,busitype FROM dw.bdl_bd_onlinead_viewlog WHERE ymd>=default.calc_date(10) AND (lower(productname) like '%card%')) view
        on show.requestid=view.v_requestid
    group by positionid,systemname,busitype,groupname
) as a
join
(
SELECT v.systemname,v.busitype,v.positionid,v.groupname,count(1) as clicks,sum(0.0001*v.bid) as income,sum(0.0001*v.bid*v.coeff) as income_2,max(0.0001*v.bid*v.coeff) as max_bid
FROM
    (SELECT requestid,ymd,count(*) from dw.bdl_bd_onlinead_clicklog  WHERE ymd>=default.calc_date(7) and ymd < default.calc_date(0)  AND (lower(productname) like '%card%') and positionid in ('SP','KNSYFW') group by requestid,ymd) as s
    left JOIN
    (SELECT requestid as v_requestid,ymd,positionid,systemname,busitype,groupname,bid,coeff FROM dw.bdl_bd_onlinead_viewlog WHERE ymd>=default.calc_date(10) AND (lower(productname) like '%card%') group by  requestid,ymd,positionid,systemname,busitype,groupname,bid,coeff ) as v
    ON s.requestid = v.v_requestid and s.ymd=v.ymd
group by positionid,systemname,groupname,busitype
)as b
on  a.systemname = b.systemname and a.busitype=b.busitype
and a.positionid = b.positionid and a.groupname=b.groupname)t
left join
dw.bdl_sms_b_account ac
on t.busitype = ac.id;





