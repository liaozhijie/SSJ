#!/bin/bash
source /home/tico/.bash_profile

cd /home/tico/zhijieliao/ssj_syyyw_ctr_wdl_emd \
    && /data/spark/spark-2.2.0/bin/spark-submit3 embedding_init.py
