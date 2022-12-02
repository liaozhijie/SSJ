#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tensorflow as tf
import numpy as np
import os,sys
import random
import pandas as pd
import configparser
from collections import defaultdict
from collections import OrderedDict
import datetime
import time
import re
import subprocess
import math
from sklearn.metrics import roc_auc_score
os.environ['TF_CPP_MIN_LOG_LEVEL']='3'
os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
starttime = datetime.datetime.now()
#model parameters
train_epochs = int(sys.argv[1])
batch_size=512
#export model True or False
exportModel=True
#copy model to tf serving
copy2Tfserving=True
#use_log
use_log=False
#model path

base_dir='/home/tico/zhijieliao/ssj_syyyw_ctr_wdl_emd'
feature_config_path="%s/model_config/ssj_syyyw_feature_rename_v3"%(base_dir)
cross_feature_config_path = "%s/model_config/ssj_cross_feature_config_v4_none"%(base_dir)

train_data_file='%s/tf_training_data/train_file_shuffle_train'%(base_dir)
test_data_file='%s/tf_training_data/train_file_shuffle_test'%(base_dir)
print(train_data_file,test_data_file)
model_export_dir='%s/export_model'%(base_dir)
print('-*-'*30)
print('model parameters \nfeature_config_path is: %s \ncross_feature_config_path is: %s \ntrain_epochs is: %s\nbatch_size is :%s'
%(feature_config_path,cross_feature_config_path,train_epochs,batch_size))

# wide columns
categorical_column_with_identity = tf.feature_column.categorical_column_with_identity
categorical_column_with_hash_bucket = tf.feature_column.categorical_column_with_hash_bucket
categorical_column_with_vocabulary_list = tf.feature_column.categorical_column_with_vocabulary_list
categorical_column_with_vocabulary_file = tf.feature_column.categorical_column_with_vocabulary_file
crossed_column = tf.feature_column.crossed_column
bucketized_column = tf.feature_column.bucketized_column
# deep columns
embedding_column = tf.feature_column.embedding_column
indicator_column = tf.feature_column.indicator_column
numeric_column = tf.feature_column.numeric_column
#print('-*-'*30)
#sequence columns
sequence_numeric_column = tf.contrib.feature_column.sequence_numeric_column
sequence_numeric_column = tf.feature_column.sequence_numeric_column
sequence_input_layer = tf.contrib.feature_column.sequence_input_layer
make_parse_example_spec = tf.feature_column.make_parse_example_spec
SequenceFeatures = tf.keras.experimental.SequenceFeatures
real_valued_column = tf.contrib.layers.real_valued_column

cf = configparser.ConfigParser()

cf.read(feature_config_path)

secs = cf.sections()

continue_cols = []
                  
for f in secs:
    if cf.get(f, "type") == 'continues':
        continue_cols.append(f)
#print(continue_cols)

def _float_feature(value):
    return tf.train.Feature(float_list=tf.train.FloatList(value=value))

def get_contiues_col_para(data_file,continue_cols):
    print('-*-'*50)
    print('start reading data')
    pd_data = pd.read_csv(data_file,sep='#',header=None,names=secs,usecols=continue_cols,na_filter=False)
    if use_log:
       print('use log(x+1) for continue_cols')
       pd_data[continue_cols] = pd_data[continue_cols].applymap(lambda x: math.log(x+1))
    contiues_col_para =defaultdict(dict)

    for col in continue_cols:
        min_value = pd.to_numeric( pd_data[col]).min()
        max_value =  pd.to_numeric(pd_data[col]).max()
        lable,bins = pd.qcut( pd.to_numeric(pd_data[col]),30,retbins=True,duplicates='drop')
        contiues_col_para[col]['min']=min_value
        contiues_col_para[col]['max']=max_value
        contiues_col_para[col]['bins']=bins
    return contiues_col_para

contiues_col_para = get_contiues_col_para(train_data_file,continue_cols)

def column_to_csv_defaults(cf):
        """parse columns to record_defaults param in tf.decode_csv func
        Return:
            OrderedDict {'feature name': [''],...}
        """
        csv_defaults = OrderedDict()
#         csv_defaults['label'] = [0]  # first label default, empty if the field is must
        secs = cf.sections()
        for f in secs:
            if cf.get(f, "type") == 'category':
                if cf.get(f,'transform') == 'identity':  # identity category column need int type
                    csv_defaults[f] = [0]
                else:
                    csv_defaults[f] = ['']
            elif cf.get(f, "type") == 'vocab_file_pre_train':
                csv_defaults[f] = ['']
            else:
                    csv_defaults[f] = [0.0]  # 0.0 for float32
        return csv_defaults

csv_defaults = column_to_csv_defaults(cf)

def parser(value,is_pred=False, field_delim='#', multivalue_delim=',',csv_defaults=csv_defaults):
    """Parse train and eval data with label
    Args:
        value: Tensor("arg0:0", shape=(), dtype=string)
    """
    # `tf.decode_csv` return rank 0 Tensor list: <tf.Tensor 'DecodeCSV:60' shape=() dtype=string>
    # na_value fill with record_defaults
    columns = tf.decode_csv(
        value, record_defaults=list(csv_defaults.values()),
        field_delim=field_delim, use_quote_delim=False)
    features = dict(zip(csv_defaults.keys(), columns))
    for f, tensor in features.items():
        if 'need_split' in cf.options(f):  # split tensor

            features[f] = tf.string_split([tensor], multivalue_delim).values  # tensor shape (?,)
        else:
            features[f] = tf.expand_dims(tensor, 0)  # change shape from () to (1,)
    labels = tf.equal(features.pop('label'), 1)
    return features, labels
    
    
def input_fn(data_file, num_epochs, shuffle, batch_size,shuffer_size_train,compress_type=""):
    assert tf.gfile.Exists(data_file), "{0} not found.".format(data_file)
    dataset = tf.data.TextLineDataset(data_file,compression_type=compress_type).map(parser, num_parallel_calls=40).prefetch(batch_size*10)
    if shuffle:
#         dataset = dataset.shuffle(buffer_size=shuffer_size_train,seed=123)
            dataset = dataset.apply(tf.contrib.data.shuffle_and_repeat(buffer_size=shuffer_size_train,count=num_epochs,seed=123))
    new_d = csv_defaults.copy()
    new_d.pop('label')
    padding_dic = {k: [None] for k in new_d.keys()}
    padded_shapes = (padding_dic, [None])
    dataset = dataset.padded_batch(batch_size,padded_shapes=padded_shapes)
    dataset = dataset.prefetch(1)
    iterator = dataset.make_one_shot_iterator()
    batch_features, batch_labels = iterator.get_next()
    return batch_features, batch_labels

def build_model_columns(feature_conf_cf,cross_cf_path):
    """
    Build wide and deep feature columns from custom feature conf using tf.feature_column API
    wide_columns: category features + cross_features + [discretized continuous features]
    deep_columns: continuous features + category features(onehot or embedding for sparse features) + [cross_features(embedding)]
    Return:
        _CategoricalColumn and __DenseColumn instance in tf.feature_column API
    """
    def embedding_dim(dim):

        """empirical embedding dim"""
#         return np.ceil((1000**0.25))
        return int(np.power(2, np.ceil(np.log(dim**0.25))))

    def normalizer_fn_builder(scaler,min_value,max_value):
        """normalizer_fn builder"""
        if scaler == 'minmax':
            return lambda x: (x-min_value) / (max_value-min_value)
        elif scaler == 'log':
            return lambda x:tf.log(x+1)
        else:
            return None
    def embedding_matrix(path):
        embed_mat = np.loadtxt(path, delimiter=',')
        return embed_mat

    def load_init_embedding(word_path, embedding_path, embedding_dim, tensor_name, ckpt_name):
        word_file = list(np.loadtxt(word_path, delimiter=','))
        word_file = [str(int(i)) for i in word_file]
        with open(base_dir+'/model_config/origid_file', 'w') as f:
            f.write('\n'.join(word_file))
        embedding_vector = np.loadtxt(embedding_path, delimiter=',', dtype=np.float32)
        embeddings = tf.Variable(initial_value=embedding_vector, dtype=tf.float32)
        init_op = tf.global_variables_initializer()
        saver = tf.train.Saver({tensor_name: embeddings})
        with tf.Session() as sess:
            sess.run(init_op)
            saver.save(sess, ckpt_name)
        embedding_initializer = tf.contrib.framework.load_embedding_initializer(
        ckpt_path=ckpt_name,
        embedding_tensor_name=tensor_name,
        new_vocab_size=len(word_file),
        embedding_dim=embedding_dim,
        old_vocab_file=base_dir+'/model_config/origid_file',
        new_vocab_file=base_dir+'/model_config/origid_file'
        )
        return embedding_initializer
    
    wide_columns = []
    deep_columns = []
    wide_dim = 0
    deep_dim = 0
    sections = feature_conf_cf.sections()
    cross_cf = configparser.ConfigParser()
    cross_cf.read(cross_cf_path)
    cross_feature_list = cross_cf.sections()
    for feature in sections:
        f_type, f_tran, f_param = feature_conf_cf.get(feature, "type"),feature_conf_cf.get(feature, "transform"),feature_conf_cf.get(feature,"parameter")
        if f_type == 'category':

            if f_tran == 'hash_bucket':
                hash_bucket_size = int(f_param)
                embed_dim = embedding_dim(hash_bucket_size)
                col = categorical_column_with_hash_bucket(feature,
                    hash_bucket_size=hash_bucket_size,
                    dtype=tf.string)
                wide_columns.append(col)
                deep_columns.append(embedding_column(col,
                    dimension=embed_dim,
                    combiner='sqrtn',
                    initializer=None,
                    ckpt_to_load_from=None,
                    tensor_name_in_ckpt=None,
                    max_norm=None,
                    trainable=True))
                wide_dim += hash_bucket_size
                deep_dim += embed_dim

            elif f_tran == 'vocab':
                col = categorical_column_with_vocabulary_list(feature,
                    vocabulary_list=f_param.split(','),
                    dtype=None,
                    default_value=-1,
                    num_oov_buckets=0)  # len(vocab)+num_oov_buckets
                wide_columns.append(col)
                embed_dim = embedding_dim(len(f_param.split(',')))
                deep_columns.append(embedding_column(col,
                    dimension=embed_dim,
                    combiner='sqrtn',
                    initializer=None,
                    ckpt_to_load_from=None,
                    tensor_name_in_ckpt=None,
                    max_norm=None,
                    trainable=True))
                deep_dim += embed_dim
#                 deep_columns.append(indicator_column(col))
                wide_dim += len(f_param)
#                 deep_dim += len(f_param)

            elif f_tran == 'vocab_file':
                fc_path = f_param
                vocabulary_size = len(open(fc_path).readlines())
                col = categorical_column_with_vocabulary_file(feature,
                    vocabulary_file=fc_path,
                    vocabulary_size=vocabulary_size,
#                     dtype=None,
                    default_value=-1,
                    num_oov_buckets=0) # len(vocab)+num_oov_buckets
                wide_columns.append(col)
                embed_dim = embedding_dim(vocabulary_size)
                if 'no_deep' not in feature_conf_cf.options(feature):
                    deep_columns.append(embedding_column(col,
                    dimension=embed_dim,
                    combiner='sqrtn',
                    initializer=None,
                    ckpt_to_load_from=None,
                    tensor_name_in_ckpt=None,
                    tensor_name_in_ckpt=None,
                    max_norm=None,
                    trainable=True))
                    deep_dim += embed_dim
                wide_dim += vocabulary_size
                if feature == 'c01':
                    emb_path = feature_conf_cf.get(feature, "parameter_emb")

                    embed_dim = 20
                    embedding_initializer_origid = load_init_embedding(fc_path, emb_path, embed_dim, 'origid_tensor', 'origid_embedding')
                    deep_columns.append(embedding_column(col,
                                                         dimension=embed_dim,
                                                         combiner='sqrtn',
                                                         initializer=embedding_initializer_origid,
                                                         ckpt_to_load_from=None,
                                                         tensor_name_in_ckpt=None,
                                                         max_norm=None,
                                                         trainable=True))
                    wide_columns.append(embedding_column(col,
                                                 dimension=embed_dim,
                                                 combiner='sqrtn',
                                                 initializer=embedding_initializer_origid,
                                                 ckpt_to_load_from=None,
                                                 tensor_name_in_ckpt=None,
                                                 max_norm=None,
                                                 trainable=True))
                    deep_dim += embed_dim
                    wide_dim += embed_dim

            elif f_tran == 'identity':
                num_buckets = int(f_param)
                col = categorical_column_with_identity(feature,
                    num_buckets=num_buckets,
                    default_value=0)  # Values outside range will result in default_value if specified, otherwise it will fail.
                wide_columns.append(col)
                embed_dim = embedding_dim(num_buckets)
                deep_columns.append(embedding_column(col,
                    dimension=embed_dim,
                    combiner='sqrtn',
                    initializer=None,
                    ckpt_to_load_from=None,
                    tensor_name_in_ckpt=None,
                    max_norm=None,
                    trainable=True))
                deep_columns.append(indicator_column(col))
                deep_dim += embed_dim
#                 deep_dim += num_buckets
                wide_dim += num_buckets

        elif f_type == 'continues':
            if f_tran == 'bucket':
                compute_boundaries = contiues_col_para[feature]['bins']
                if use_log:
                   log_fn = lambda x:tf.log(x+1)
                else:
                   log_fn=None
                boundaries = para_boundaries = list(map(float,f_param.split(','))) if f_param != '' else list(compute_boundaries)
                col = numeric_column(feature,
                 shape=(1,),
                 default_value=0,  # default None will fail if an example does not contain this column.
                 dtype=tf.float32,
                 normalizer_fn=log_fn)
                bucket_col = bucketized_column(col, boundaries=boundaries)
                wide_columns.append(bucket_col)
                embed_dim = embedding_dim(len(boundaries))
                deep_columns.append(embedding_column(bucket_col,
                    dimension=embed_dim,
                    combiner='sqrtn',
                    initializer=None,
                    ckpt_to_load_from=None,
                    tensor_name_in_ckpt=None,
                    tensor_name_in_ckpt=None,
                    max_norm=None,
                    trainable=True))
                deep_dim += embed_dim
                wide_dim += len(boundaries)+1
            elif f_tran == 'value':
                 col = numeric_column(feature,
                 shape=(1,),
                 default_value=0,  # default None will fail if an example does not contain this column.
                 dtype=tf.float32,
                 normalizer_fn=log_fn)
                 wide_columns.append(col)
                 deep_columns.append(col)
                 deep_dim +=1
                 wide_dim +=1

        elif f_type == 'vocab_file_pre_train':
            fc_path = f_param
            vocabulary_size = len(open(fc_path).readlines())
            emb_path = feature_conf_cf.get(feature, "parameter_emb")
            embed_dim = 20
            embedding_initializer_click = load_init_embedding(fc_path, emb_path, embed_dim, 'click_tensor', 'click_embedding')
            vocabulary_size = len(open(fc_path).readlines())
            col = categorical_column_with_vocabulary_file(feature,
                                                          vocabulary_file=fc_path,
                                                          vocabulary_size=vocabulary_size,
                                                          default_value=0,
                                                          num_oov_buckets=0)  # len(vocab)+num_oov_buckets
            deep_columns.append(embedding_column(col,
                                                 dimension=embed_dim,
                                                 combiner='sqrtn',
                                                 initializer=embedding_initializer_click,
                                                 ckpt_to_load_from=None,
                                                 tensor_name_in_ckpt=None,
                                                 max_norm=None,
                                                 trainable=True))
            wide_columns.append(embedding_column(col,
                                                 dimension=embed_dim,
                                                 combiner='sqrtn',
                                                 initializer=embedding_initializer_click,
                                                 ckpt_to_load_from=None,
                                                 tensor_name_in_ckpt=None,
                                                 max_norm=None,
                                                 trainable=True))
            deep_dim += embed_dim
            wide_dim += embed_dim


    for cross_features_str in cross_feature_list:
        cf_list = []
        cross_features = cross_features_str.split('&')
        hash_bucket_size = cross_cf.getint(cross_features_str, "hash_bucket_size")
        is_deep = cross_cf.get(cross_features_str, "is_deep")
#         print(cross_features)
        for feature in cross_features:
            f_type, f_tran, f_param = feature_conf_cf.get(feature, "type"),feature_conf_cf.get(feature, "transform"),feature_conf_cf.get(feature,"parameter")
#             print(feature,f_type, f_tran, f_param)
            if f_type == 'continues':
#                 print(feature)
                boundaries = contiues_col_para[feature]['bins']
#                 print(boundaries)
                cf_list.append(bucketized_column(numeric_column(feature, default_value=0), boundaries=list(boundaries)))
            else:
                if f_tran == 'identity':
                    # If an input feature is of numeric type, you can use categorical_column_with_identity
                    num_buckets = int(f_param)
                    col = categorical_column_with_identity(feature,
                    num_buckets=num_buckets,
                    default_value=0)
                    cf_list.append(col)
                elif f_tran == 'hash_bucket':
                    hash_bucket_size = int(f_param)
                    embed_dim = embedding_dim(hash_bucket_size)
                    col = categorical_column_with_hash_bucket(feature,
                    hash_bucket_size=hash_bucket_size,
                    dtype=tf.string)
                    cf_list.append(col)
                elif f_tran == 'vocab':
                    col = categorical_column_with_vocabulary_list(feature,
                    vocabulary_list=f_param.split(','),
                    dtype=None,
                    default_value=-1,
                    num_oov_buckets=0)
                    cf_list.append(col)
                elif f_tran == 'vocab_file':
                    fc_path = f_param
                    vocabulary_size = len(open(fc_path).readlines())
                    col = categorical_column_with_vocabulary_file(feature,
                    vocabulary_file=fc_path,
                    vocabulary_size=vocabulary_size,
#                     dtype=None,
                    default_value=-1,
                    num_oov_buckets=0) # len(vocab)+num_oov_buckets
                    cf_list.append(col)
    # category col put the name in crossed_column
#         tf.logging.info(cf_list)
#         print(cf_list)
        col = crossed_column(cf_list, hash_bucket_size)
        wide_columns.append(col)
        wide_dim += hash_bucket_size
        if is_deep == '1':
            deep_columns.append(embedding_column(col,combiner='sqrtn', dimension=embedding_dim(hash_bucket_size)))
            deep_dim += embedding_dim(hash_bucket_size)
    # add columns logging info
    #tf.logging.info('Build total {} wide columns'.format(len(wide_columns)))
    print('Build total {} wide columns'.format(len(wide_columns)))
    for col in wide_columns:
        tf.logging.debug('Wide columns: {}'.format(col))
    #tf.logging.info('Build total {} deep columns'.format(len(deep_columns)))
    print('Build total {} deep columns'.format(len(deep_columns)))
    for col in deep_columns:
        tf.logging.debug('Deep columns: {}'.format(col))
    #tf.logging.info('Wide input dimension is: {}'.format(wide_dim))
    #tf.logging.info('Deep input dimension is: {}'.format(deep_dim))
    print('Wide input dimension is: {}'.format(wide_dim))
    print('Deep input dimension is: {}'.format(deep_dim))


    return wide_columns, deep_columns
                                                         
                                                         
                                                         
                                                         
                                                         
                                                         
                                                         
def export_model(feature_columns,export_dir):
    feature_spec = tf.feature_column.make_parse_example_spec(feature_columns)
    serving_input_receiver_fn = tf.estimator.export.build_parsing_serving_input_receiver_fn(feature_spec)
    outpath=deepwidemodel.export_savedmodel(export_dir,serving_input_receiver_fn)
    return outpath

#get wide and deep columns 
wide_columns, deep_columns=build_model_columns(feature_conf_cf=cf,cross_cf_path=cross_feature_config_path)
print('-*-'*50)
print('start build model')
#构建模型
deep_and_wide_model_dir='%s/model_check_online/%s'%(base_dir,int(time.time()))
print("model check dir is %s"%(deep_and_wide_model_dir))
deepwidemodel = tf.estimator.DNNLinearCombinedClassifier(
    model_dir = deep_and_wide_model_dir,
    linear_feature_columns=wide_columns,
    linear_optimizer=tf.train.FtrlOptimizer(
                learning_rate=0.01,
                l1_regularization_strength=0.001,
                l2_regularization_strength=0.1),
    linear_sparse_combiner='sqrtn',
    dnn_feature_columns=deep_columns,
    dnn_hidden_units=[256,128,64],
    dnn_dropout = 0.5,
    dnn_optimizer=tf.train.AdagradOptimizer(
                learning_rate=0.1
            )
    #dnn_optimizer=tf.train.ProximalAdagradOptimizer(
    #learning_rate=0.01,
    #l1_regularization_strength=0.001,
    #l2_regularization_strength=0.1)
     ,batch_norm = True
)
print('-*-'*50)
print('start training')

def gauc(df,key):
    filter_df=df.groupby(key).filter(lambda df: np.unique(df['label']).size>1)
    auc_df=filter_df.groupby(key).apply(lambda df: (df[df['label'] == 0].size)*roc_auc_score(df['label'],df['pro'])).reset_index()
    auc_df.columns=[key,'auc']
    return auc_df['auc'].sum()/filter_df[filter_df['label']==0].size

for n in range(train_epochs):
    deepwidemodel.train(input_fn=lambda:input_fn(train_data_file,1,False,batch_size,6000000,compress_type=""))
    if True:
        results_test = deepwidemodel.evaluate(input_fn=lambda:input_fn(test_data_file,1,False,batch_size,3000000,compress_type=""))
        #results_train = deepwidemodel.evaluate(input_fn=lambda:input_fn(train_data_file,1,False,batch_size,4000000,compress_type=""))
        # Display Eval results
        print("Results at epoch {0}".format((n)))
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print('-'*30)
        print("train_evaluate")
        #for key in sorted(results_train):
        #    print("{0:20}: {1:.4f}".format(key, results_train[key]))
        print("\ntest_evaluate")
        for key in sorted(results_test):
            print("{0:20}: {1:.4f}".format(key, results_test[key]))
# GAUC
results = deepwidemodel.predict(input_fn=lambda:input_fn(train_data_file,1,False,1024,400000))
probability = [i['probabilities'][1] for i in results]
predict_df = pd.read_csv(train_data_file,sep='#',header=None,names=secs,na_filter=False)
predict=predict_df[['udid','label']]
predict['pro']=probability
result = gauc(predict,'udid')
print("\nGAUC: ",result)

print('-*-'*50)
print('finish training')

#predict_result = deepwidemodel.predict(input_fn=lambda:input_fn(test_data_file,1,False,batch_size,1000000,compress_type=""))
#print("predict_result")
#predict_list = []
#print("predict_start_time:",datetime.datetime.now().strftime('%Y-%m-%d'))
#for p in predict_result:
#	predict_list.append(p['probabilities'])
#print("predict_end_time:",datetime.datetime.now().strftime('%Y-%m-%d'))
#sys.exit(-1)


if exportModel:
    print('-*-'*50)
    print('start export model')
    feature_column=wide_columns+deep_columns
    outpath=export_model(feature_column,model_export_dir)
    outpath=bytes.decode(outpath)
    print('export model to %s'%(outpath))
    print('finish export model')

print('-*-'*50)
endtime = datetime.datetime.now()
print ('runing time is %s'%(endtime - starttime))

if copy2Tfserving:
        print('copy model  to tf servering')
        tf_model_path='/home/hadoop/models/ssj_wdl_syyyw_v4/'
        tf_model_path_0='/home/hadoop/models/ssj_wdl_syyyw_v4/0'
        least_version=subprocess.getoutput('ls -t %s| head -1'%(tf_model_path_0))
        insert_version= int(least_version)+1
        tf_output_model_0='%s/%s'%(tf_model_path_0,insert_version)
        tf_output_model='%s/%s'%(tf_model_path,insert_version)
        subprocess.run('cp -r %s  %s'%(outpath,tf_output_model_0),shell=True)
        subprocess.run('cp -r %s  %s ; chmod -R +777 %s'%(tf_output_model_0,tf_output_model,tf_output_model),shell=True)
#	subprocess.run('cd /home/hadoop/models;sh update_wdl_model.sh',shell=True)
        print('-*-'*50)
        print('the least_version is %s  ;  the insert model is %s'%(least_version,tf_output_model))
        print('-*-'*50)
        time.sleep(30)
        subprocess.run('cd /home/hadoop/models;sh update_wdl_model.sh update "{\\"token\\":\\"cb9b9b0fe59974cd\\",\\"hdfsPath\\":\\"ssj_wdl_syyyw_v4:8103\\",\\"modelTypes\\":\\"tensorflow\\",\\"creator\\":\\"zhijieliao\\",\\"description\\":\\"ssj_wdl_syyyw_v4\\",\\"poolSize\\":0,\\"source\\":\\"manual\\"}"',shell=True)
#print('-*-'*50)
#print('create check_file to tf servering')

#version = re.findall(r"\d+",outpath)[0]

#file_size=commands.getoutput('du -s -b %s | cut -d"\t" -f 1'%(outpath))

#check_file=model_export_dir+'/check_file'

#print("%s\t%s"%(version,file_size))

#with open(check_file,"w") as f:
#        f.write("%s\t%s"%(version,file_size))
#os.system("scp -P16120  ./tf_20181107  zhijieliao@10.129.53.23:/data/zhijieliao/cpd_data_online/tf_1_5_data/")
#print('finish scp export model')
