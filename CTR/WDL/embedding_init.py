# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession
import datetime
import pandas as pd
import numpy as np

import os

spark = SparkSession \
    .builder \
    .appName('wdl_embedding_collect') \
    .config("spark.sql.warehouse.dir", '/user/hive/warehouse') \
    .enableHiveSupport() \
    .getOrCreate()



BEELINE = """beeline -u \"jdbc:hive2://zk-common1.suishoushuju.internal:2181,zk-common2.suishoushuju.internal:2181,zk-common3.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common4.suishoushuju.internal:2181,zk-common5.suishoushuju.internal:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2\" -n fdbd_ad -p vbbtar72 """


if __name__ == '__main__':
        base_dir='/home/tico/zhijieliao/ssj_syyyw_ctr_wdl_emd/'
        origid_embedding_df = spark.sql("select * from intelli_algo.ssj_origid_embedding").toPandas()
        with open(base_dir + 'model_config/origid_config', 'r') as f:
                with open(base_dir + 'model_config/origid_embedding', 'w') as w:
                                for l in f:
                                        origid = l.strip('\n')
                                        vector = origid_embedding_df[origid_embedding_df['origid'] == origid]['origid_embedding']
                                        vector = ('0,'*20).strip(',') if len(vector) == 0 else vector.values[0]
                                        w.write(vector + '\n')
