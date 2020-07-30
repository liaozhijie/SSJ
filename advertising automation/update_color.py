#python2
# -*- coding: utf-8 -*-
import datetime
import sys
import os
import numpy as np
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import cv2
from PIL import Image
import urllib2
from PIL import ImageStat
from collections import Counter
import ast
import collections
import json

#RGB和16进制转换
def RGB_to_Hex(tmp):
    strs = '#'
    for i in tmp:
        strs += str(hex(i))[-2:].replace('x', '0').upper()
    return strs

#提取TOP颜色
def show_color(color_num,limit,block,pil_image):
    small_image = pil_image.resize((80, 80))
    result = small_image.convert('P', palette=Image.ADAPTIVE, colors=color_num).convert('RGB')
    main_colors = result.getcolors(80 * 80)
    rechange_color=[]
    for count, col in sorted(main_colors,key=lambda x:x[0],reverse=True):
        if count < limit:
            continue
        rechange_color.append((int(col[0]/block)*block if col[0] < 250 else 200,int(col[1]/block)*block if col[1] < 250 else 200,int(col[2]/block)*block if col[2] < 250 else 200))
    showed_list = []
    for i in rechange_color:
        if_print = 1
        if i not in showed_list:
            for j in showed_list:
                count = 0
                for k in range(3):
                    if i[k] == j[k]:
                        count += 1
                if count == 2:
                    if abs(sum(i) - sum(j)) <= block:
                        if_print = 0
                        break
            if if_print:
                showed_list.append(i)
    res_list=[RGB_to_Hex(i) for i in showed_list]
    return main_colors,rechange_color,showed_list,res_list

#颜色鲜艳度
def image_colorfulness(cv_image):
    #将图片分为B,G,R三部分（注意，这里得到的R、G、B为向量而不是标量）
    (B, G, R) = cv2.split(cv_image.astype("float"))
    rg = np.absolute(R - G)
    yb = np.absolute(0.5 * (R + G) - B)
    #计算rg和yb的平均值和标准差
    (rbMean, rbStd) = (np.mean(rg), np.std(rg))
    (ybMean, ybStd) = (np.mean(yb), np.std(yb))
    #计算rgyb的标准差和平均值
    stdRoot = np.sqrt((rbStd ** 2) + (ybStd ** 2))
    meanRoot = np.sqrt((rbMean ** 2) + (ybMean ** 2))
    return stdRoot + (0.3 * meanRoot)

#图片亮度
def image_brightness(pil_image):
    im = pil_image.convert('L')
    stat = ImageStat.Stat(im)
    return (stat.mean[0])

#人脸检测
def image_facedetect(cv_image):
    image = cv_image
    detector = cv2.CascadeClassifier("haarcascade_frontalface_default.xml")
    rects = detector.detectMultiScale(image, scaleFactor=1.1, minNeighbors=4, minSize=(10, 10),flags=cv2.CASCADE_SCALE_IMAGE)
    return int(len(rects) > 0)

#各个颜色比例
def get_color_rate(pil_image,rgb_list):
    if (0,0,0) in rgb_list:
        rgb_list.remove((0,0,0))
    if not rgb_list:
        return {}
    width = pil_image.width
    height = pil_image.height
    image_list = []
    for x in range(height):
        for y in range(width):
            pixel = pil_image.getpixel((y, x))
            image_list.append(pixel)
    image_list_dict = Counter(image_list)
    rate_res = [0] * len(rgb_list)
    for i in range(len(rgb_list)):
        for k in image_list_dict.keys():
            b = 1
            for j in range(3):
                if k[j] >= rgb_list[i][j]:
                    if k[j] < 200 and k[j] <= rgb_list[i][j] + 50:
                        continue
                else:
                    b = 0
                    break
            if b:
                rate_res[i] += image_list_dict[k]
    rate_res = np.array(rate_res) / float(sum(rate_res))
    dict = {}
    for i in range(len(rate_res)):
        if rate_res[i] < 0.05:
            continue
        dict[str(rgb_list[i])] = float('%.2f' % rate_res[i])
    return dict

#图片抓取
def get_image(image_url):
        image_url = 'https://downloads.feidee.com/sq' + image_url
        request = urllib2.Request(image_url)
        response = urllib2.urlopen(request)
        read = response.read()
        cv_image = cv2.imdecode((np.asarray(bytearray(read), dtype="uint8")), cv2.IMREAD_COLOR)
        pil_image = Image.fromarray(cv2.cvtColor(cv_image,cv2.COLOR_BGR2RGB))
        return cv_image, pil_image

def get_icon_df(url):
    cv_image,pil_image = get_image(url)
    if cv_image == '':
        return '', '', '', ''
    colorfulness = image_colorfulness(cv_image)
    icon_rgb = show_color(10,0,50,pil_image)[2][:5]
    color_rate = json.dumps(get_color_rate(pil_image,icon_rgb)).replace(' ','')
    face_detect = image_facedetect(cv_image)
    brightness = image_brightness(pil_image)
    return colorfulness, color_rate, face_detect, brightness

def split_background_color(pil_image):
    w,h = pil_image.size
    box1 = (0,0,w,int(h/2))
    box2 = (0,int(h/2),w,h)
    im1 = pil_image.crop(box1)
    im2 = pil_image.crop(box2)
    im2.show()
    d1 = get_color_rate(im1,show_color(10,0,50,im1)[2][:3])
    d2 = get_color_rate(im2,show_color(10,0,50,im2)[2][:3])
    if not d1:
        d1 = {'(0,0,0)':0.5}
    if not d2:
        d2 = {'(1,0,0)':0.5}
    r1,r2 = sorted(d1.items(),key = lambda x:x[1],reverse = True)[0][0],sorted(d2.items(),key = lambda x:x[1],reverse = True)[0][0]
    #r1 = tuple([int(i.strip(' ')) for i in r1.strip('(').strip(')').split(',')])
    #r2 = tuple([int(i.strip(' ')) for i in r2.strip('(').strip(')').split(',')])
    return r1,r2

def get_background_df(url):
    cv_image,pil_image = get_image(url)
    if cv_image == '':
        return '', '', '', ''
    colorfulness = image_colorfulness(cv_image)
    background_top_color,background_low_color = split_background_color(pil_image)
    color_rate = "{\"" + background_top_color + "\":0.5,\"" + background_low_color + "\":0.5}"
    face_detect = "null"
    brightness = image_brightness(pil_image)
    return colorfulness, color_rate, face_detect, brightness
		
def get_button_df(url):
    colorfulness = "null"
    cv_image,pil_image = get_image(url)
    if cv_image == '':
        return '', '', '', ''
    d = get_color_rate(pil_image,show_color(10,0,50,pil_image)[2][:3])
    if not d:
        d = {"(0,0,0)":1.0}
    color_rate = json.dumps({sorted(d.items(),key = lambda x:x[1],reverse = True)[0][0]:1.0}).replace(' ','')
    face_detect = "null"
    brightness = "null"
    return colorfulness, color_rate, face_detect, brightness




if __name__ == "__main__":
    with open("lock.txt",'r+') as f:
        for l in f:
            if l:
                sys.exit(-1)
        else:
            f.write("1")

    try:
        feature_df = pd.read_csv("t_fodder_automation_trait.txt",sep='\t')
        feature_df = feature_df[feature_df['status'] == 1]
        if len(feature_df) > 0:
            t = datetime.datetime.now()
            icon_count = 0
            background_count = 0
            button_count = 0
            for i in range(len(feature_df)):
                url = feature_df.iloc[i]['image_url']
                element_code = feature_df.iloc[i]['element_code']
                try:
                    if "BACKGROUND" in element_code:
                        colorfulness, color_rate, face_detect, brightness = get_background_df(url)
                        background_count += 1
                    elif "ICON" in element_code:
                        colorfulness, color_rate, face_detect, brightness = get_icon_df(url)
                        icon_count += 1
                    elif "TEMPLATE" in element_code:
                        colorfulness, color_rate, face_detect, brightness = get_button_df(url)
                        button_count += 1
                    else:
                        print "Error:element_code error, %s" % element_code
                    color_rate = color_rate.replace(' ','').replace('\"','\\"')
                    update_sql = "update splash.t_fodder_automation_trait set status=2, color_fulness = %s, color_rate = '%s', face_detect = %s, brightness = %s where element_code = '%s'" % (str(colorfulness)[:5],color_rate,face_detect,str(brightness)[:5],element_code)
                    if colorfulness != '':
                        os.system("mysql -hsplash1.dsjfeideedba.com -uSQ_splash_idc -p,b6233cd__36656c,cc5,28d80ef -P3307 --default-character-set=utf8 -e \"%s\"" % update_sql)
                except Exception as run_e:
                    update_sql = "update splash.t_fodder_automation_trait set status = 3 where element_code = '%s'" % element_code
                    print "Error,run error,%s, element_code = %s, url = %s" % (run_e,element_code, url)
                    os.system("mysql -hsplash1.dsjfeideedba.com -uSQ_splash_idc -p,b6233cd__36656c,cc5,28d80ef -P3307 --default-character-set=utf8 -e \"%s\"" % update_sql)
            if (datetime.datetime.now() - t).seconds > 0:
                print "[NOT Error]run_time: ",(datetime.datetime.now() - t).seconds
                print "[NOT Error]icon_num: ",icon_count
                print "[NOT Error]background_num: ",background_count
                print "[NOT Error]button_num: ",button_count
    except Exception as e:
        print "Error: ",e
    finally:
        with open("lock.txt",'w') as f:
            pass
	
