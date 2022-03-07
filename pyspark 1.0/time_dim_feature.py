# -*- coding: utf-8 -*-

import datetime
from pyspark.sql import HiveContext, Row
from pyspark import SparkContext
from pyspark.sql.types import *
import sys

def get_feature_collect(data_rdd):
        for row in data_rdd.collect():
                udid = str(row['udid'])
                positionid = str(row['positionid'])
                if udid != '0277ceac6aaa7fdb054c671de5464fca54f7bc251b495c0b33f25d08930dd7ac0b915f0d2bf85ce95c436c3503b46703' or positionid != 'SP':
                        continue

                show_info = row['show_info']
                print "show_info",show_info

                show_thesame_origid_didnot_click_num = 0
                show_didnot_click_num = 0
                show_didnot_click_daynum = 0
                last_show_gap_day = 0
                continue_click_num = 0

                show_list = show_info.split(',')
                show_dict = {}
                for s in show_list:
                        showtime, clicktime, origid = s.split('#')
                        show_dict[showtime] = [clicktime, origid]
                sort_list = [(k,show_dict[k]) for k in sorted(show_dict.keys(),reverse=True)]
                print "sort_list:",sort_list
                origid = sort_list[0][1][1]
                if_show_didnot_click_num = 1
                if_show_didnot_click_daynum = 1
                if_continue_click_num = 1
                for item in sort_list:
                        showtime = item[0]
                        clicktime = item[1][0]
                        origid1 = item[1][1]
                        if origid1 == origid and clicktime == "-1" and origid != -1:
                                show_thesame_origid_didnot_click_num += 1
                        else:
                                origid = -1
                        if clicktime == "-1" and if_show_didnot_click_num:
                                show_didnot_click_num += 1
                        else:
                                if_show_didnot_click_num = 0
                                if if_show_didnot_click_daynum:
                                        show_didnot_click_daynum = (build_day - datetime.datetime.strptime(clicktime,'%Y-%m-%d %H:%M:%S')).days
                                        print "show_didnot_click_daynum",show_didnot_click_daynum,
                                        print "build_day",build_day,"clicktime",clicktime
                                        if_show_didnot_click_daynum = 0
                        if if_show_didnot_click_daynum and if_show_didnot_click_num:
                                show_didnot_click_daynum = back_day_num - 1
                        if clicktime != "-1" and if_continue_click_num:
                                continue_click_num += 1
                        else:
                                if_continue_click_num = 0

                last_show_gap_day = (build_day - datetime.datetime.strptime(sort_list[0][0],'%Y-%m-%d %H:%M:%S')).days + 1
                print "sort_list[0][0]",sort_list[0][0]
                print "build_day",build_day
                print "datetime.datetime.strptime(sort_list[0][0],'%Y-%m-%d %H:%M:%S')",datetime.datetime.strptime(sort_list[0][0],'%Y-%m-%d %H:%M:%S')
                print "last_show_gap_day",last_show_gap_day
                #return Row(udid, positionid, build_day.strftime('%Y-%m-%d'), show_thesame_origid_didnot_click_num, show_didnot_click_num, show_didnot_click_daynum, last_show_gap_day, continue_click_num)
                
                
                def get_feature(row):
        data_dict = row.asDict()
        udid = data_dict['udid']
        positionid = data_dict['positionid']
        show_info = data_dict['show_info']

        show_thesame_origid_didnot_click_num = 0
        show_didnot_click_num = 0
        show_didnot_click_daynum = 0
        last_show_gap_day = 0
        continue_click_num = 0

        show_list = show_info.split(',')
        show_dict = {}
        for s in show_list:
                if len(s.split('#')) != 3:
                        continue
                showtime, clicktime, origid = s.split('#')
                show_dict[showtime] = [clicktime, origid]
        sort_list = [(k,show_dict[k]) for k in sorted(show_dict.keys(),reverse=True)]
        if not sort_list:
                return Row(udid, positionid, build_day.strftime('%Y-%m-%d'), 0, 0, 11, 11, 0)
        origid = sort_list[0][1][1]
        if_show_didnot_click_num = 1
        if_show_didnot_click_daynum = 1
        if_continue_click_num = 1
        for item in sort_list:
                showtime = item[0]
                clicktime = item[1][0]
                origid1 = item[1][1]
                if origid1 == origid and clicktime == "-1" and origid != -1:
                        show_thesame_origid_didnot_click_num += 1
                else:
                        origid = -1
                if clicktime == "-1" and if_show_didnot_click_num:
                        show_didnot_click_num += 1
                else:
                        if_show_didnot_click_num = 0
                        if if_show_didnot_click_daynum:
                                #if build_day.strftime('%H:%M:%S') < clicktime.split(' ')[1]:
                                #       show_didnot_click_daynum = (build_day - datetime.datetime.strptime(clicktime,'%Y-%m-%d %H:%M:%S')).days + 1
                                #else:
                                show_didnot_click_daynum = (datetime.datetime.strptime(build_day.strftime('%Y-%m-%d'),'%Y-%m-%d') - datetime.datetime.strptime(clicktime.split(' ')[0],'%Y-%m-%d')).days
                                if_show_didnot_click_daynum = 0
                if clicktime != "-1" and if_continue_click_num:
                        continue_click_num += 1
                else:
                        if_continue_click_num = 0
        if if_show_didnot_click_daynum and if_show_didnot_click_num:
                show_didnot_click_daynum = 11
        #if build_day.strftime('%H:%M:%S') < sort_list[0][0].split(' ')[1]:
        #       last_show_gap_day = (build_day - datetime.datetime.strptime(sort_list[0][0],'%Y-%m-%d %H:%M:%S')).days + 1
        #else:
        last_show_gap_day = (datetime.datetime.strptime(build_day.strftime('%Y-%m-%d'),'%Y-%m-%d') - datetime.datetime.strptime(sort_list[0][0].split(' ')[0],'%Y-%m-%d')).days
        return Row(udid, positionid, build_day.strftime('%Y-%m-%d'), show_thesame_origid_didnot_click_num, show_didnot_click_num, show_didnot_click_daynum if show_didnot_click_daynum<=11 else 11, last_show_gap_day if last_show_gap_day <= 11 else 11, continue_click_num)
      
def get_feature(row):
        data_dict = row.asDict()
        udid = data_dict['udid']
        positionid = data_dict['positionid']
        show_info = data_dict['show_info']

        show_thesame_origid_didnot_click_num = 0
        show_didnot_click_num = 0
        show_didnot_click_daynum = 0
        last_show_gap_day = 0
        continue_click_num = 0

        show_list = show_info.split(',')
        show_dict = {}
        for s in show_list:
                if len(s.split('#')) != 3:
                        continue
                showtime, clicktime, origid = s.split('#')
                show_dict[showtime] = [clicktime, origid]
        sort_list = [(k,show_dict[k]) for k in sorted(show_dict.keys(),reverse=True)]
        if not sort_list:
                return Row(udid, positionid, build_day.strftime('%Y-%m-%d'), 0, 0, 11, 11, 0)
        origid = sort_list[0][1][1]
        if_show_didnot_click_num = 1
        if_show_didnot_click_daynum = 1
        if_continue_click_num = 1
        for item in sort_list:
                showtime = item[0]
                clicktime = item[1][0]
                origid1 = item[1][1]
                if origid1 == origid and clicktime == "-1" and origid != -1:
                        show_thesame_origid_didnot_click_num += 1
                else:
                        origid = -1
                if clicktime == "-1" and if_show_didnot_click_num:
                        show_didnot_click_num += 1
                else:
                        if_show_didnot_click_num = 0
                        if if_show_didnot_click_daynum:
                                #if build_day.strftime('%H:%M:%S') < clicktime.split(' ')[1]:
                                #       show_didnot_click_daynum = (build_day - datetime.datetime.strptime(clicktime,'%Y-%m-%d %H:%M:%S')).days + 1
                                #else:
                                show_didnot_click_daynum = (datetime.datetime.strptime(build_day.strftime('%Y-%m-%d'),'%Y-%m-%d') - datetime.datetime.strptime(clicktime.split(' ')[0],'%Y-%m-%d')).days
                                if_show_didnot_click_daynum = 0
                if clicktime != "-1" and if_continue_click_num:
                        continue_click_num += 1
                else:
                        if_continue_click_num = 0
        if if_show_didnot_click_daynum and if_show_didnot_click_num:
                show_didnot_click_daynum = 11
        #if build_day.strftime('%H:%M:%S') < sort_list[0][0].split(' ')[1]:
        #       last_show_gap_day = (build_day - datetime.datetime.strptime(sort_list[0][0],'%Y-%m-%d %H:%M:%S')).days + 1
        #else:
        last_show_gap_day = (datetime.datetime.strptime(build_day.strftime('%Y-%m-%d'),'%Y-%m-%d') - datetime.datetime.strptime(sort_list[0][0].split(' ')[0],'%Y-%m-%d')).days
        return Row(udid, positionid, build_day.strftime('%Y-%m-%d'), show_thesame_origid_didnot_click_num, show_didnot_click_num, show_didnot_click_daynum if show_didnot_click_daynum<=11 else 11, last_show_gap_day if last_show_gap_day <= 11 else 11, continue_click_num)
      
      
      
      
def combine_positionid(row):
        devssj_sp_same_origid_didnot_click_num,devssj_sp_didnot_click_num,devssj_sp_last_click_gap_day,devssj_sp_last_show_gap_day,devssj_sp_continue_click_num,devssj_ssjsyyyw_same_origid_didnot_click_num,devssj_ssjsyyyw_didnot_click_num,devssj_ssjsyyyw_last_click_gap_day,devssj_ssjsyyyw_last_show_gap_day,devssj_ssjsyyyw_continue_click_num,devssj_ssjsybn_same_origid_didnot_click_num,devssj_ssjsybn_didnot_click_num,devssj_ssjsybn_last_click_gap_day,devssj_ssjsybn_last_show_gap_day,devssj_ssjsybn_continue_click_num = [0]*15
        devssj_sp_last_click_gap_day,devssj_sp_last_show_gap_day,devssj_ssjsyyyw_last_click_gap_day,devssj_ssjsyyyw_last_show_gap_day,devssj_ssjsybn_last_click_gap_day,devssj_ssjsybn_last_show_gap_day = [11]*6
        udid = row['fudid']
        positionid_feature_list = row['udid_feature'].split('$')
        for i in positionid_feature_list:
                positionid = i.split('|')[0]
                if positionid == "SP":
                        devssj_sp_same_origid_didnot_click_num,devssj_sp_didnot_click_num,devssj_sp_last_click_gap_day,devssj_sp_last_show_gap_day,devssj_sp_continue_click_num = [int(k) for k in i.split('|')[1:]]
                elif positionid == "SSJSYYYW":
                        devssj_ssjsyyyw_same_origid_didnot_click_num,devssj_ssjsyyyw_didnot_click_num,devssj_ssjsyyyw_last_click_gap_day,devssj_ssjsyyyw_last_show_gap_day,devssj_ssjsyyyw_continue_click_num = [int(k) for k in i.split('|')[1:]]
                elif positionid == "SSJSYBN":
                        devssj_ssjsybn_same_origid_didnot_click_num,devssj_ssjsybn_didnot_click_num,devssj_ssjsybn_last_click_gap_day,devssj_ssjsybn_last_show_gap_day,devssj_ssjsybn_continue_click_num = [int(k) for k in i.split('|')[1:]]
        return Row(udid,devssj_sp_same_origid_didnot_click_num,devssj_sp_didnot_click_num,devssj_sp_last_click_gap_day,devssj_sp_last_show_gap_day,devssj_sp_continue_click_num,devssj_ssjsyyyw_same_origid_didnot_click_num,devssj_ssjsyyyw_didnot_click_num,devssj_ssjsyyyw_last_click_gap_day,devssj_ssjsyyyw_last_show_gap_day,devssj_ssjsyyyw_continue_click_num,devssj_ssjsybn_same_origid_didnot_click_num,devssj_ssjsybn_didnot_click_num,devssj_ssjsybn_last_click_gap_day,devssj_ssjsybn_last_show_gap_day,devssj_ssjsybn_continue_click_num)

def build_feature_7d(sc, hc, input_table_name, output_table_name):
        nowday = datetime.datetime.now()
        train_data_num = 7
        res_feature = sc.parallelize([])
        global build_day
        global back_day_num
        back_day_num = 10
        for i in list(range(30,0,-1)):
                build_day = nowday - datetime.timedelta(days=i)
                print "build_day",build_day
                back_day = build_day - datetime.timedelta(days=back_day_num)
                print "back_day",back_day
                sql_str1 = "create table temp.temp_build_time_feature_ssj as select * from %s where dt>'%s' and dt<='%s'" % (input_table_name,  back_day.strftime('%Y-%m-%d'), build_day.strftime('%Y-%m-%d'))
                print sql_str1
                sql_str2 = "create table temp.temp_build_time_feature_ssj_concat as select t.udid,t.positionid,concat_ws(',',collect_set(t.s_c_o)) as show_info from (select u.udid,u.positionid, concat(u.showtime,'#',u.clicktime,'#',u.origid) as s_c_o from (select * from %s where dt>='%s' and dt<='%s') u) t group by t.udid,t.positionid" % (input_table_name,  back_day.strftime('%Y-%m-%d'), build_day.strftime('%Y-%m-%d'))
                hc.sql("drop table if exists temp.temp_build_time_feature_ssj")
                # hc.sql(sql_str1)
                hc.sql("drop table if exists temp.temp_build_time_feature_ssj_concat")
                hc.sql(sql_str2)
                # temp_data = hc.table("temp.temp_build_time_feature_ssj_concat").rdd.filter(lambda x:str(x['udid']) == "0277ceac6aaa7fdb054c671de5464fca54f7bc251b495c0b33f25d08930dd7ac0b915f0d2bf85ce95c436c3503b46703")
                temp_data = hc.table("temp.temp_build_time_feature_ssj_concat")
                # res_feature = res_feature.union(temp_data.map(get_feature))
                res_data = temp_data.map(get_feature)
                schema = ["fudid", "fpositionid", "fymd", "show_thesame_origid_didnot_click_num", "show_didnot_click_num", "last_click_gap_day" ,"last_show_gap_day", "continue_click_num"]
                save_data = hc.createDataFrame(res_data,schema = schema)
                save_data.registerTempTable("res_feature")
                hc.sql("drop table if exists temp.res_feature")
                hc.sql("create table temp.res_feature as select t.fudid,concat_ws('$',collect_set(t.positionid_feature)) as udid_feature from (select fudid,concat(fpositionid,'|',show_thesame_origid_didnot_click_num,'|',show_didnot_click_num,'|',last_click_gap_day,'|',last_show_gap_day,'|',continue_click_num) as positionid_feature from res_feature) t group by t.fudid")
                data_res = hc.table("temp.res_feature").map(combine_positionid)
                schema1 = ["fudid","devssj_sp_same_origid_didnot_click_num","devssj_sp_didnot_click_num","devssj_sp_last_click_gap_day","devssj_sp_last_show_gap_day","devssj_sp_continue_click_num","devssj_ssjsyyyw_same_origid_didnot_click_num","devssj_ssjsyyyw_didnot_click_num","devssj_ssjsyyyw_last_click_gap_day","devssj_ssjsyyyw_last_show_gap_day","devssj_ssjsyyyw_continue_click_num","devssj_ssjsybn_same_origid_didnot_click_num","devssj_ssjsybn_didnot_click_num","devssj_ssjsybn_last_click_gap_day","devssj_ssjsybn_last_show_gap_day","devssj_ssjsybn_continue_click_num"]
                sava_data_res =  hc.createDataFrame(data_res,schema = schema1)
                sava_data_res.registerTempTable("res_feature_1")
                hc.sql("insert overwrite table %s partition(dt='%s') select * from res_feature_1" % (output_table_name, (build_day + datetime.timedelta(days=1)).strftime("%Y-%m-%d")))
                # get_feature_collect(temp_data)

                
def create_partition_table(sc,hc):
        table_name = "temp.ad_ssj_join_all_feature_base_gauc20"
        data = hc.sql("describe temp.ad_ssj_join_all_feature_base_gauc20").collect()
        s=''
        for i in data:
                col_name = str(i['col_name'])
                data_type = str(i['data_type'])
                s = s + ',' + col_name + ' ' + data_type
        s = s[1:]
        s = "udid string,positionid string,showtime string,clicktime string,origid string"
        s_f = "udid,positionid,showtime,clicktime,origid"
        sql_str = "create table intelli_algo.ad_ssj_join_all_feature_base_gauc20_p (%s) partitioned by(dt string)" % s
        hc.sql("drop table if exists intelli_algo.ad_ssj_join_all_feature_base_gauc20_p")
        hc.sql(sql_str)
        for d in ['2019-12-11','2019-12-10','2019-12-09','2019-12-08','2019-12-07','2019-12-06','2019-12-05','2019-12-04','2019-12-03', '2019-12-02', '2019-12-01', '2019-11-30', '2019-11-29', '2019-11-28', '2019-11-27', '2019-11-26', '2019-11-25', '2019-11-24', '2019-11-23', '2019-11-22']:
                hc.sql("insert overwrite table intelli_algo.ad_ssj_join_all_feature_base_gauc20_p partition(dt='%s') select %s from temp.ad_ssj_join_all_feature_base_gauc20 where train_ymd='%s'" % (d, s_f, d))


def update_feature(sc, hc, input_update_table_name, output_table_name):
        global build_day
        build_day = datetime.datetime.now()
        global back_day_num
        back_day_num = 10
        back_day = build_day - datetime.timedelta(days=back_day_num)
        data = hc.sql("describe intelli_algo.ad_ssj_join_all_feature_base_gauc20_p").collect()
        s=''
        for i in data:
                col_name = str(i['col_name'])
                if col_name != "dt" and '#' not in col_name:
                        s = s + ',' + col_name
        s = s[1:]
        hc.sql("insert overwrite table intelli_algo.ad_ssj_join_all_feature_base_gauc20_p partition(dt='%s') select %s from %s" % (build_day.strftime("%Y-%m-%d"), s, input_update_table_name))
        sql_str2 = "create table temp.temp_build_time_feature_ssj_concat as select t.udid,t.positionid,concat_ws(',',collect_set(t.s_c_o)) as show_info from (select u.udid,u.positionid, concat(u.showtime,'#',u.clicktime,'#',u.origid) as s_c_o from (select * from %s where dt>='%s' and dt<='%s') u) t group by t.udid,t.positionid" % (input_table_name,  back_day.strftime('%Y-%m-%d'), build_day.strftime('%Y-%m-%d'))
        hc.sql("drop table if exists temp.temp_build_time_feature_ssj_concat")
        hc.sql(sql_str2)
        temp_data = hc.table("temp.temp_build_time_feature_ssj_concat")
        res_data = temp_data.map(get_feature)
        schema = ["fudid", "fpositionid", "fymd", "show_thesame_origid_didnot_click_num", "show_didnot_click_num", "last_click_gap_day" ,"last_show_gap_day", "continue_click_num"]
        save_data = hc.createDataFrame(res_data,schema = schema)
        save_data.registerTempTable("res_feature")
        hc.sql("insert overwrite table %s partition(dt='%s') select * from res_feature" % ("intelli_algo.ssj_ad_time_dimention_feature", (build_day + datetime.timedelta(days=1)).strftime("%Y-%m-%d")))
        hc.sql("drop table if exists temp.res_feature")
        hc.sql("create table temp.res_feature as select t.fudid,concat_ws('$',collect_set(t.positionid_feature)) as udid_feature from (select fudid,concat(fpositionid,'|',show_thesame_origid_didnot_click_num,'|',show_didnot_click_num,'|',last_click_gap_day,'|',last_show_gap_day,'|',continue_click_num) as positionid_feature from res_feature) t group by t.fudid")
        data_res = hc.table("temp.res_feature").map(combine_positionid)
        schema1 = ["fudid","devssj_sp_same_origid_didnot_click_num","devssj_sp_didnot_click_num","devssj_sp_last_click_gap_day","devssj_sp_last_show_gap_day","devssj_sp_continue_click_num","devssj_ssjsyyyw_same_origid_didnot_click_num","devssj_ssjsyyyw_didnot_click_num","devssj_ssjsyyyw_last_click_gap_day","devssj_ssjsyyyw_last_show_gap_day","devssj_ssjsyyyw_continue_click_num","devssj_ssjsybn_same_origid_didnot_click_num","devssj_ssjsybn_didnot_click_num","devssj_ssjsybn_last_click_gap_day","devssj_ssjsybn_last_show_gap_day","devssj_ssjsybn_continue_click_num"]
        sava_data_res =  hc.createDataFrame(data_res,schema = schema1)
        sava_data_res.registerTempTable("res_feature_1")

        hc.sql("insert overwrite table %s partition(dt='%s') select * from res_feature_1" % (output_table_name, (build_day + datetime.timedelta(days=1)).strftime("%Y-%m-%d")))
        drop_date = build_day - datetime.timedelta(days=60)
        hc.sql("alter table %s drop partition(dt='%s')" % (output_table_name, drop_date.strftime("%Y-%m-%d")))
        hc.sql("drop table if exists intelli_algo.ssj_ad_time_dimention_feature_online")
        hc.sql("create table intelli_algo.ssj_ad_time_dimention_feature_online as select * from res_feature_1")
        
        
        
        
        
        
if __name__ == "__main__":
        sc = SparkContext()
        hc = HiveContext(sc)
        #create_partition_table(sc,hc)
        input_table_name = "intelli_algo.ad_ssj_join_all_feature_base_gauc20_p"
        output_table_name = "intelli_algo.ssj_ad_time_dimention_feature_new"
        #res = build_feature_7d(sc, hc, input_table_name, output_table_name)
        #sys.exit(-1)

        input_update_table_name = "temp.ad_ssj_join_all_feature_base_gauc_update_timefeature"
        update_feature(sc, hc, input_update_table_name, output_table_name)


        # schema = ["fudid", "fpositionid", "fymd", "show_thesame_origid_didnot_click_num", "show_didnot_click_num", "last_click_gap_day" ,"last_show_gap_day", "continue_click_num"]
        # save_data = hc.createDataFrame(res, schema = schema)
        # hc.sql("DROP TABLE IF EXISTS %s" %(output_table_name))
        # save_data.write.saveAsTable(output_table_name, mode = "overwrite")
