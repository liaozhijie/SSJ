# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession
import datetime
import pandas as pd
import numpy as np
from gensim.models import Word2Vec
import os

spark = SparkSession \
    .builder \
    .appName('ssj_ctr_graph_embedding') \
    .config("spark.sql.warehouse.dir", '/user/hive/warehouse') \
    .enableHiveSupport() \
    .getOrCreate()

BEELINE = """beeline -u \"jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -n fdbd_ad -p vbbtar72 """

class Edge:
    def __init__(self, source, destine):
        self.source_weight = 1
        self.source = source
        self.destine = destine
        self.edge_weight = 1

class Graph:
    def __init__(self):
        self.edges = {}
        self.pre_nodes = set([])

    def add_edge(self, edge):
        if edge.source not in self.pre_nodes:
            self.pre_nodes.add(edge.source)
            self.edges[edge.source] = []
        for i in range(len(self.edges[edge.source])):
            self.edges[edge.source][i].source_weight += 1
            if edge.destine == self.edges[edge.source][i].destine:
                self.edges[edge.source][i].edge_weight += 1
        if edge.destine not in [i.destine for i in self.edges[edge.source]]:
            self.edges[edge.source].append(edge)

    def get_edges_num(self):
        return len(self.edges)

def weight_random_choice(edges_list, target_name):
    source = [i.source if target_name == "source_weight" else i.destine for i in edges_list]
    source_weight = [i.source_weight for i in edges_list]
    edge_weight = [i.edge_weight for i in edges_list]
    weight = source_weight if target_name == "source_weight" else edge_weight
    value = np.random.random() * sum(weight)
    for i in range(len(weight)):
        if sum(weight[:i]) < value and sum(weight[:i + 1]) >= value:
            return source[i]
    print ("Error,random_choice not return")

def build_graph(graph, user_click_data):
    for click_seq in user_click_data:
        #点击序列去重
        click_seq = [click_seq[i] for i in range(len(click_seq)) if i == 0 or (i > 0 and click_seq[i] != click_seq[i - 1])]
        if len(click_seq) > 100 or len(click_seq) < 2:
            continue
        for i in range(len(click_seq) - 1):
            graph.add_edge(Edge(click_seq[i], click_seq[i+1]))
    return graph

def generate_click_seq(graph,walk_len,total_generate):
    result_list = []
    for t in range(total_generate):
        start_node = weight_random_choice([graph.edges[i][0] for i in graph.pre_nodes], "source_weight")
        walk_result = [start_node]
        while len(walk_result) < walk_len:
            try:
                cur_node = weight_random_choice(graph.edges[walk_result[-1]], "edge_weight")
                walk_result.append(cur_node)
            except:
                break
        result_list.append(walk_result)
    return result_list

def get_user_embedding(w2v_model, user_udid_seq, user_click_seq, user_time_seq):
    not_cover_origid = set([])
    result_emd = []
    for i in range(len(user_click_seq)):
        emd_i = []
        seq = user_click_seq[i][-5:]
        for s in range(len(seq)):
            try:
                emd_i.append(np.array(w2v_model[seq[s]]))
            except:
                emd_i.append(np.array([0]*20))
                not_cover_origid.add(seq[s])
        weight = ((1 / np.array(user_time_seq[i])) / sum(1 / np.array(user_time_seq[i])))
        result_i = list(list(emd_i[j] * weight[j] for j in range(len(emd_i)))[0])
        if i<5:
            print (result_i)
        result_emd.append(str(result_i).strip('[]'))
    for i in range(len(result_emd)):
        s= ''
        t = ['%.4f' % float(j.strip('\n')) for j in result_emd[i].split(', ') if j != '']
        for k in t:
            s = s + k + ','
        result_emd[i] = s.strip(',')
    print ("not_cover_origid: ",len(not_cover_origid))
    df = pd.DataFrame()
    df['udid'] = user_udid_seq
    df['click_emd'] = result_emd
    return df


def get_origid_embedding(w2v_model, origid_list):
    result_emd = []
    for i in origid_list:
        try:
            result_emd.append(str(w2v_model[i]).strip('[]'))
        except:
            result_emd.append(('0 '*20).strip(' '))
    for i in range(len(result_emd)):
        s= ''
        t = ['%.4f' % float(j.strip('\n')) for j in result_emd[i].split(' ') if j != '']
        for k in t:
            s = s + k + ','
        result_emd[i] = s.strip(',')

    df = pd.DataFrame()
    df['origid'] = origid_list
    df['origid_vec'] = result_emd
    return df

def build_days_user_embedding(w2v_model,back_days):
    for num in range(back_days):
        back_day = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=num-1),'%Y-%m-%d')
        print ("running %s" % back_day)
        sql_str = """create table temp.ssj_click_seq_30 as select 
                    udid,concat_ws(',',collect_list(origid)) as click_seq , concat_ws(',',collect_list(clicktime)) as time_seq
                from 
                    (select *,row_number() over (distribute by udid sort by clicktime asc) from dw.bdl_bd_onlinead_clicklog where ymd>='{backtime}' and ymd <= default.calc_date({back_num}) and lower(productname) like '%mymoney%' and positionid in ('SP','SSJSYYYW')) t  
                group by udid""".format(backtime=datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=30+num), '%Y-%m-%d'),back_num=num)
        os.system(BEELINE + "-e \"drop table if exists temp.ssj_click_seq_30\"")
        os.system(BEELINE + "-e \"%s\"" % sql_str)
        user_data_df = spark.sql("select * from temp.ssj_click_seq_30").toPandas()
        user_udid_seq = [i for i in user_data_df['udid']]
        user_click_seq = [i.split(',') for i in user_data_df['click_seq']]
        user_time_seq = [[(datetime.datetime.now() - datetime.timedelta(days=num) - datetime.datetime.strptime(j,'%Y-%m-%d %H:%M:%S')).days for j in i.split(',')] for i in user_data_df['time_seq']]
        udid_embedding_df = get_user_embedding(w2v_model, user_udid_seq, user_click_seq, user_time_seq)
        user_df = spark.createDataFrame(udid_embedding_df)
        user_df.coalesce(1).write.csv(path='hdfs://nameservice1/user/algo/guifu/liaozhijie/user_embedding_%s' % back_day,sep='\t', mode='overwrite')
        user_str = BEELINE + """-e \"load data inpath \'hdfs://nameservice1/user/algo/guifu/liaozhijie/user_embedding_{today}\' overwrite into table intelli_algo.ssj_user_click_embedding partition(dt = \'{today}\')\"""".format(today=back_day)
        os.system(user_str)
if __name__ == "__main__":
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    sql_str = """create table temp.ssj_user_click_seq as select 
                udid,concat_ws(',',collect_list(origid)) as click_seq , concat_ws(',',collect_list(clicktime)) as time_seq
            from 
                (select *,row_number() over (distribute by udid sort by clicktime asc) from dw.bdl_bd_onlinead_clicklog where ymd>='{backtime}' and ymd <= default.calc_date(0) and lower(productname) like '%mymoney%' and positionid in ('SP','SSJSYYYW')) t  
            group by udid""".format(backtime = datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(days=30),'%Y-%m-%d'))
    os.system(BEELINE + "-e \"drop table if exists temp.ssj_user_click_seq\"")
    os.system(BEELINE + "-e \"%s\"" % sql_str)
    user_data_df = spark.sql("select * from temp.ssj_user_click_seq").toPandas()
    user_udid_seq = [i for i in user_data_df['udid']]
    user_click_seq = [i.split(',') for i in user_data_df['click_seq']]
    user_time_seq = [[(datetime.datetime.now() - datetime.datetime.strptime(j,'%Y-%m-%d %H:%M:%S')).days for j in i.split(',')] for i in user_data_df['time_seq']]

    #生成图和游走序列
    graph = build_graph(Graph(), user_click_seq)
    print ("total origid num:",len(graph.pre_nodes))
    t=datetime.datetime.now()
    walk_list = generate_click_seq(graph, 5, 500000)
    print ("walk cost time :", (datetime.datetime.now() - t).seconds)
    ll=[]
    for i in walk_list:
        ll+=i
    print ("total walk origid:",len(set(ll)))

    #训练词向量
    t = datetime.datetime.now()
    w2v_model = Word2Vec(walk_list, size = 20, window = 2, min_count = 1, negative = 5, sg=1, hs=0)
    print("word2vec cost time :", (datetime.datetime.now() - t).seconds)

    origid_embedding_df = get_origid_embedding(w2v_model, list(graph.pre_nodes))
    origid_df = spark.createDataFrame(origid_embedding_df)
    origid_df.coalesce(1).write.csv(path='hdfs://nameservice1/user/algo/guifu/liaozhijie/origid_embedding_%s' % today,sep='\t', mode='overwrite')
    origid_str = BEELINE + """-e \"load data inpath \'hdfs://nameservice1/user/algo/guifu/liaozhijie/origid_embedding_{today}\' overwrite into table intelli_algo.ssj_origid_embedding\"""".format(today=today)
    os.system(origid_str)

    build_days_user_embedding(w2v_model, 10)


'''
create table intelli_algo.ssj_origid_embedding(origid string,origid_vec string) comment "随手记广告embedding" row format delimited fields terminated by '\t' stored as   textfile;
create table intelli_algo.ssj_user_click_embedding(udid string,click_emd string) comment "随手记用户点击序列embedding" partitioned by (dt string)  row format delimited fields terminated by '\t' stored as   textfile;
'''