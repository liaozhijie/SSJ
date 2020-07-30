#python2
# -*- coding: utf-8 -*-

import datetime
from pyspark.sql import HiveContext, Row
from pyspark import SparkContext
from pyspark.sql.types import *
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )
import numpy as np
import pandas as pd
import cv2
from PIL import Image
import urllib2
from PIL import ImageStat
from pyspark.sql.types import StructField, StringType, FloatType, StructType
import ast
import math


def RGB_to_Hex(tmp):
	strs = '#'
	for i in tmp:
		strs += str(hex(i))[-2:].replace('x', '0').upper()
	return strs

def feature_process(row):
	origid = row['origid']
	ctr = row['ctr']
	text_list_group = row['text_list_group']
	business_type = row['business_type']
	background_model = ['background_model']
	image_url = 'https://downloads.feidee.com/sq' + str(row['image_url'])
	element_code_list = row['element_code_list']

	# 分开字符串
	main_title, sub_title, button_copywriter, template_name, background_color, background_type, background_model, icon_name, tag_name_list, icon_resource, background_resource = ['-1'] * 11
	for i in text_list_group.split('**'):
		text_list_i = i.split('##')[1:]
		if "COPYWRITER" in i:
			main_title, sub_title, button_copywriter = text_list_i[:3]
			tag_name_list = tag_name_list.split('-1')[0] + text_list_i[8]
		elif "ICON" in i:
			icon_name, icon_resource = text_list_i[7], text_list_i[9]
		elif "TEMPLATE" in i:
			template_name, background_color, background_type, background_model = text_list_i[3:7]
		elif "BACKGROUND" in i:
			background_resource = text_list_i[10]
	template_code, background_code, icon_code = "", "" ,""
	for i in element_code_list.split('|'):
		if "ICON" in i:
			icon_code = i
		elif "TEMPLATE" in i:
			template_code = i
		elif "BACKGROUND" in i:
			background_code = i
	icon_colorfulness,icon_color_rate,background_color_rate,button_color_rate,face_detect,background_brightness,icon_brightness, background_colorfulness, button_color = ["0"] * 9
	icon_colorfulness,background_brightness,icon_brightness,background_colorfulness = [0] * 4
	if icon_code:
		color_data = color_data_df[color_data_df['element_code'] == icon_code].values
		if len(color_data):
			icon_colorfulness,icon_color_rate,face_detect,icon_brightness = color_data[0][1:5]
	if template_code:
		color_data = color_data_df[color_data_df['element_code'] == template_code].values
		if len(color_data):
			button_color_rate = color_data[0][2]
	if background_code:
		color_data = color_data_df[color_data_df['element_code'] == background_code].values
		if len(color_data):
			background_colorfulness, background_color_rate,background_brightness = color_data[0][1], color_data[0][2], color_data[0][4]
	if button_color_rate != "0":
		button_color_rate_dict = ast.literal_eval(button_color_rate)
		rgb_str = button_color_rate_dict.keys()[0]
		tuple_rgb = tuple([int(i.strip(' ')) for i in rgb_str.strip('(').strip(')').split(',')])
		button_color = RGB_to_Hex(tuple_rgb)
	_c = icon_resource
	
	return Row(origid,ctr,text_list_group,business_type,str(row['image_url']),'%.2f' % icon_colorfulness,'%.2f' % background_colorfulness,icon_color_rate,background_color_rate,button_color_rate,'%.2f' % background_brightness,'%.2f' % icon_brightness,button_color,face_detect,main_title,sub_title,button_copywriter,template_name, background_color, background_type, background_model, icon_name, tag_name_list, _c, background_resource)



# 本地处理部分

def get_tag_name_onehot(tag_name_list,word):
	res_list = []
	for i in range(len(tag_name_list)):
		tag_name_list_i = tag_name_list[i]
		res_list.append(int(word in tag_name_list_i))
	return res_list

def get_tag_name_list(sc,hc,table_name):
	tag_collect = hc.sql("select distinct tag_name from %s" % table_name).collect()
	tag_name_list = []
	for i in tag_collect:
		tag_name_list.append(str(i['tag_name']))
	return tag_name_list

def get_color_split():
	res_list = []
	rgb = ['0','50','100','150','200']
	for r in rgb:
		for g in rgb:
			for b in rgb:
				res_list.append(r+'|'+g+'|'+b)
	return res_list

def get_color_onehot(icon_color_rate,color_split):
	df = pd.DataFrame()
	for c in color_split:
		df['icon_color_' + c] = []
	for i in range(len(icon_color_rate)):
		df_t = pd.DataFrame()
		for c in color_split:
			df_t['icon_color_' + c] = [0]
		if not icon_color_rate[i] or icon_color_rate[i] == '0':
			df = df.append(df_t)
			continue
		try:
			icon_color_rate_i = ast.literal_eval(icon_color_rate[i])
		except:
			print icon_color_rate[i]
			df = df.append(df_t)
			continue
		for k in icon_color_rate_i.keys():
			tup = tuple([int(i.strip(' ')) for i in k.strip('(').strip(')').split(',')])
			df_t['icon_color_' + str(tup[0]) + '|' + str(tup[1]) + '|' + str(tup[2])] = [float(icon_color_rate_i[k])]
		df = df.append(df_t)
	return df

def ColourDistance(rgb_1, rgb_2):
	R_1, G_1, B_1 = rgb_1
	R_2, G_2, B_2 = rgb_2
	rmean = (R_1 + R_2) / 2
	R = R_1 - R_2
	G = G_1 - G_2
	B = B_1 - B_2
	return math.sqrt((2 + rmean / 256) * (R ** 2) + 4 * (G ** 2) + (2 + (255 - rmean) / 256) * (B ** 2))

def compute_distance(dict1,dict2):
	if dict1 == 0 or dict2 == 0:
		return '-1'
	dis = 0
	for it1 in dict1.items():
		for it2 in dict2.items():
			rgb_1 = tuple([int(i.strip(' ')) for i in it1[0].strip('(').strip(')').split(',')])
			rate_1 = float(it1[1])
			rgb_2 = tuple([int(i.strip(' ')) for i in it2[0].strip('(').strip(')').split(',')])
			rate_2 = float(it2[1])
			dis += ColourDistance(rgb_1,rgb_2) * rate_1 * rate_2
	return str(dis)

def element_distance(df_icon,df_background,df_button):
	icon_background,background_button,icon_button = [],[],[]
	for i in range(len(df_icon)):
		try:
			icon_dict = ast.literal_eval(df_icon[i])
		except:
			icon_dict = {"(200, 200, 200)": 1.0}
		background_dict = ast.literal_eval(df_background[i])
		button_dict = ast.literal_eval(df_button[i])
		background_button_dis = compute_distance(background_dict,button_dict)
		#不计算icon_button_dis
		#icon_button_dis = compute_distance(icon_dict,button_dict)
		icon_button_dis = '-1'
		icon_background_dis = compute_distance(icon_dict,background_dict)
		icon_background.append(icon_background_dis)
		background_button.append(background_button_dis)
		icon_button.append(icon_button_dis)
	return icon_background,background_button,icon_button
		


if __name__ == "__main__":
	sc = SparkContext()
	hc = HiveContext(sc)
	global color_data_df
	color_data_df = pd.read_csv("feature_table.txt",sep='\t').drop(['status'],axis=1).fillna(0)
	origid_table = "temp.auto_ad_make_total_feature_code"
	data = hc.table(origid_table)
	res_data = data.map(feature_process)
	struct_field_list = []
	columns_name = ['origid','ctr','text_list_group','business_type','image_url','icon_colorfulness','background_colorfulness','icon_color_rate','background_color_rate','button_color_rate','background_brightness','icon_brightness','button_color','face_detect','main_title','sub_title','button_copywriter','template_name','background_color','background_type','background_model','icon_name','tag_name_list','icon_resource','background_resource']
	for name in columns_name:
		struct_field_list.append(StructField(name, StringType(), True))
	schema = StructType(struct_field_list)
	save_data = hc.createDataFrame(res_data,schema = schema)
	df = save_data.toPandas()
	df.to_csv("res_data1.csv",index=False)
	
	tag_name_list = get_tag_name_list(sc,hc,"dw.bdl_splash_fodder_automation_tag")
	color_split = get_color_split()
	for word in tag_name_list:
		df['tag_name_%s' % word] = get_tag_name_onehot(df['tag_name_list'],word)
	color_df = get_color_onehot(df['icon_color_rate'],color_split)
	df['icon_background_distance'],df['background_button_distance'],df['icon_button_distance'] = element_distance(df['icon_color_rate'],df['background_color_rate'],df['button_color_rate'])
	df = pd.concat([df, color_df.reset_index(drop=True)],axis=1)
	df.to_csv("res_data2.csv",index=False,sep=',',encoding='utf-8')
	feature_dict = {}
	with open('rename_table_columns.txt','r') as f:
		for l in f:
			k,v = l.strip('\n').split(' : ')
			feature_dict[k] = v

	reverse_dict = dict([(v,k) for (k,v) in feature_dict.iteritems()])
	table_columns = "origid,label,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11,s12,s13,s14,s15,s16,s17,s18,s19,s20,s21,s22,s23,s24,s25,s26,s27,s28,s29,s30,s31,s32,s33,s34,s35,s36,s37,s38,s39,s40,s41,s42,s43,s44,s45,s46,s47,s48,s49,s50,s51,s52,s53,s54,s55,s56,s57,s58,s59,s60,s61,s62,s63,s64,s65,s66,s67,s68,s69,s70,s71,s72,s73,s74,s75,s76,s77,s78,s79,s80,s81,s82,s83,s84,s85,s86,s87,s88,s89,s90,s91,s92,s93,s94,s95,s96,s97,s98,s99,s100,s101,s102,s103,s104,s105,s106,s107,s108,s109,s110,s111,s112,s113,s114,s115,s116,s117,s118,s119,s120,s121,s122,s123,s124,s125,s126,s127,s128,s129,s130,s131,s132,s133,s134,s135,s136,s137,s138,s139,s140,s141,s142,s143,s144,s145,s146,s147,s148,s149,s150,s151,s152,s153,s154,s155,s156,s157,s158,s159,s160,s161,s162,s163,s164,s165,s166,s167,s168,s169,s170,s171,s172,s173,s174,s175,s176,s177,s178,s179,s180,s181,s182,s183,s184,s185,s186,s187,s188,s189,s190,s191,s192,s193,s194,s195,s196,s197,s198,s199,s200,s201,s202,s203,s204,s205,s206,s207,s208,s209,s210,s211,s212,s213,s214,s215,s216,s217,s218,s219"
	df.rename(columns=feature_dict,inplace=True)
	for v in feature_dict.values():
		if v not in df.columns:
			df[v] = [0] * len(df)

	#转为分类
	median = df['ctr'].median()
	df['label'] = [1*(float(i) > median) for i in df['ctr'].tolist()]
	df = df.drop(['ctr'],axis=1)

	df =df.sample(frac=1)
	df_train = df[:int(len(df) * 0.8)]
	df_test = df[int(len(df) * 0.8):]
	df_values_train = df_train.values.tolist()
	df_values_test = df_test.values.tolist()
	df_columns = list(df.columns)
	spark_df_train = hc.createDataFrame(df_values_train,df_columns)
	spark_df_test = hc.createDataFrame(df_values_test,df_columns)
	spark_df_train.registerTempTable("res_train")
	spark_df_test.registerTempTable("res_test")
	hc.sql("insert overwrite table intelli_algo.ssj_ad_auto_train_data partition(dt='%s') select %s from res_train" % (datetime.datetime.now().strftime('%Y-%m-%d'),table_columns))
	hc.sql("insert overwrite table intelli_algo.ssj_ad_auto_test_data partition(dt='2020-03-20') select %s from res_test" % (table_columns))





