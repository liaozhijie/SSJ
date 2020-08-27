# -*- coding:utf-8 -*-
import smtplib
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email import encoders
from pyspark.sql import SparkSession
import datetime
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns; sns.set()
import datetime
import xlsxwriter

#画图中文显示问题
plt.rcParams['font.sans-serif']=['SimHei']
sns.set(font='SimHei')
plt.rcParams['axes.unicode_minus']=False

spark = SparkSession \
    .builder \
    .appName('ctr_weekly_report') \
    .config("spark.sql.warehouse.dir", '/user/hive/warehouse') \
    .enableHiveSupport() \
    .getOrCreate()

#运营位信息
POSITIONID_DICT = {'SHFWYZBB2': '随手记主产品-生活服务页中部banner2', 'SHFWYZBB1': '随手记主产品-生活服务页中部banner1',
                        'SHFWYZBB3': '随手记主产品-生活服务页中部banner3', 'LCSYTT4': '随手记主产品-理财首页头图4',
                        'LCSYTT3': '随手记主产品-理财首页头图3', 'LCSYTT1': '随手记主产品-理财首页头图1', 'LCSYTT2': '随手记主产品-理财首页头图2',
                        'LCSYXF': '随手记主产品-理财首页悬浮icon', 'LCSYB5': '随手记主产品-理财首页banner5',
                        'LCSYB2': '随手记主产品-理财首页banner2', 'LCSYB4': '随手记主产品-理财首页banner4',
                        'LCSYB3': '随手记主产品-理财首页banner3', 'SHFWYDBTT': '随手记主产品-生活服务页顶部贴图',
                        'LCSYB1': '随手记主产品-理财首页banner', 'LCSYTC': '随手记主产品-理财首页弹窗', 'ZTSYLBW': '随手记主产品-主题首页轮播位',
                        'SSJSQB': '随手记主产品-随手记社区banner', 'SHFWCFSYB1': '生活服务财富首页banner1', 'SP': '闪屏',
                        'SSJSYYYW': '首页运营位', 'SSJZHSYYYW': '随手记主产品-随手记账户首页运营位',
                        'SSJTBTBXQY': '随手记主产品-随手记图表-图表详情页运营位', 'SSJTBTXTY': '随手记主产品-随手记图表-条形图页运营位',
                        'SSJSYLCZXKP': '随手记主产品-随手记首页理财资讯卡片', 'SSJXYZBSY': '随手记主产品-信用账本首页运营位',
                        'SSJSYSQKP': '随手记主产品-随手记首页社区卡片', 'SSJLSLBY': '随手记主产品-随手记流水列表页运营位', 'XXGDW': '社区-消息固定位',
                        'LM-TT':'LM-TT(穿山甲)','LM-YLH':'LM-YLH(优量汇)'}

#实验组信息
sp_line = ['NO_TCTR_Group_BIGDATA','CTR_GBDT_Model_DJ','SSJ_CVR_MODEL_DJ','SSJ_WDL_SYYYW_DJ','SSJ_WDL_DJ','SSJ_WDL_SP_DJ','SSJ_DEEPFM_V1_DJ']
syyyw_line = ['NO_TCTR_Group_BIGDATA','CTR_GBDT_Model_DJ','SSJ_CVR_MODEL_DJ','SSJ_WDL_SYYYW_DJ','SSJ_WDL_DJ','SSJ_WDL_V2_DJ']
line_describe_dict = {'NO_TCTR_Group_BIGDATA':'随机组','CTR_GBDT_Model_DJ':'新手用户流量','SSJ_CVR_MODEL_DJ':'转化率模型','SSJ_WDL_SYYYW_DJ':'WDL baseline','SSJ_WDL_DJ':'WDL 实验组','SSJ_WDL_SP_DJ':'WDL 实验组','SSJ_DEEPFM_V1_DJ':'deepFM实验模型','SSJ_WDL_V2_DJ':'WDL 实验组'}
sp_rate_dict = {'NO_TCTR_Group_BIGDATA':'10%','CTR_GBDT_Model_DJ':'-','SSJ_CVR_MODEL_DJ':'15%','SSJ_WDL_SYYYW_DJ':'60%','SSJ_WDL_DJ':'5%','SSJ_WDL_SP_DJ':'5%','SSJ_DEEPFM_V1_DJ':'5%'}
syyyw_rate_dict = {'NO_TCTR_Group_BIGDATA':'10%','CTR_GBDT_Model_DJ':'-','SSJ_CVR_MODEL_DJ':'15%','SSJ_WDL_SYYYW_DJ':'65%','SSJ_WDL_DJ':'5%','SSJ_WDL_V2_DJ':'5%'}

#RECEIVERS=['zhijie_liao@sui.com','haotao_lin@sui.com','zhe_feng@sui.com','min_zhuo@sui.com','ying_gu@sui.com']
RECEIVERS=['zhijie_liao@sui.com']



def send_email(title, mix_content, picture_attach_list, attach_path):
    mail_host = "mail.sui.com"
    mail_user = "zhijie_liao"
    mail_pass = "yhzaMFBU8hYz2fgv"
    MAIL_POSTFIX = "sui.com"
    REPORT_MAILTO_LIST = RECEIVERS
    sender = 'zhijie_liao@sui.com'

    message = MIMEMultipart()
    for i in range(len(picture_attach_list)):
        message.attach(picture_attach_list[i])
    content_text = MIMEText(mix_content, 'html', _charset='utf-8')
    message.attach(content_text)
    #添加附件
    xlsxpart = MIMEApplication(open(attach_path, 'rb').read())
    xlsxpart.add_header('Content-Disposition', 'attachment', filename=('gbk','','%s~%s随手记内部广告曝光数据汇总.xlsx' % (datetime.datetime.strftime((datetime.datetime.strptime(today, '%Y-%m-%d') - datetime.timedelta(days=7)),'%Y-%m-%d'),datetime.datetime.now().strftime('%Y-%m-%d'))))
    message.attach(xlsxpart)

    message['Subject'] = Header(title,"utf-8")
    message['To'] = ";".join(RECEIVERS)
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.login(mail_user, mail_pass)
        server.sendmail(sender, RECEIVERS, message.as_string())
        server.close()
        print("send email secceed")
        return True
    except Exception:
        print ("Error: send email fail")
        return False

def build_picture_html(picture_path, sign, picture_title):
    pic_html = """
        <html>
            <head></head>
            <body>
                <p><b>%s</b>:
                    <br><img src="cid:%s"></br>
                </p>
            </body>
        </html>
        """ % (picture_title,sign)
    attach = MIMEImage(open(picture_path, 'rb').read())
    attach.add_header('Content-ID', '<%s>' % sign)
    return pic_html,attach

def get_picture_set(picture_path_list):
    html_sp_busitype_ctr,p_sp_busitype_ctr = build_picture_html(picture_path_list[0],"image5","随手记闪屏曝光量点击率（分业务线）")
    html_syyyw_busitype_ctr,p_syyyw_busitype_ctr = build_picture_html(picture_path_list[1],"image7","随手记首页运营位曝光量点击率（分业务线）")
    html_sp_system_ctr,p_sp_system_ctr = build_picture_html(picture_path_list[2],"image9","随手记闪屏曝光量点击率（分操作系统）")
    html_syyyw_system_ctr,p_syyyw_system_ctr = build_picture_html(picture_path_list[3],"image11","随手记首页运营位曝光量点击率（分操作系统）")
    return [html_sp_busitype_ctr,html_syyyw_busitype_ctr,html_sp_system_ctr,html_syyyw_system_ctr],[p_sp_busitype_ctr,p_syyyw_busitype_ctr,p_sp_system_ctr,p_syyyw_system_ctr]

def email_content_mix(picture_path_list, html_table_list, head_msg, tail_msg, summary_word):
    html_picture_list, picture_attach_list = get_picture_set(picture_path_list)
    mix_content = head_msg + summary_word[0] + html_table_list[0] + html_table_list[1] + html_picture_list[0] + \
                  html_picture_list[2] + summary_word[1] + html_table_list[2] + html_table_list[3] + html_picture_list[
                      1] + html_picture_list[3] + summary_word[2] + html_table_list[4] + summary_word[3] + \
                  html_table_list[5] + tail_msg
    return mix_content, picture_attach_list

def get_span_num_list(data_list):
    result = []
    init = data_list[0]
    count = 0
    for i in range(len(data_list)):
        if data_list[i] == init:
            count += 1
        else:
            result.append(count)
            init = data_list[i]
            count = 1
    result.append(count)
    return result

#Dataframe 转 html_table
def get_html_msg(df, table_title, merge_columns_list):
    # 表格格式
    head = \
        """
        <head>
            <meta charset="utf-8">
            <STYLE TYPE="text/css" MEDIA=screen>

                table.dataframe {
                    border-collapse:collapse;
                    border: 2px solid #a19da2;
                    /*默认居中auto显示整个表格*/
                    margin: left
                }

                table.dataframe thead {
                    border: 2px solid #333333;
                    background: #f1f1f1;
                    padding: 10px 10px 10px 10px;
                    color: #333333;
                }

                table.dataframe tbody {
                    border: 2px solid #333333;
                    padding: 10px 10px 10px 10px;
                }

                table.dataframe tr {
                }

                table.dataframe th {
                    vertical-align: top;
                    font-size: 14px;
                    padding: 10px 10px 10px 10px;
                    color: #333333;
                    font-family: arial;
                    text-align: center;
                }

                table.dataframe td{
                    text-align: left;
                    padding: 10px 10px 10px 10px;
                }

                body {
                    font-family: 宋体；
                }

                h1 {
                    color: #333333
                    }

                div.header h2 {
                    color: #333333;

                div.content h2 {
                    text-align: center;
                    font-size: 28px;
                    text-shadow: 2px 2px 1px #de4040;
                    color: #fff;
                    font-weight: bold;
                    background-color: #333333;
                    line-height: 1.5;
                    margin: 20px 0;
                    box-shadow: 10px 10px 5pxx #888888;
                    border-radius: 5px;
                }

                h3 {
                    font-size: 22px;
                    background-color: rgba(0,2,227,0.71);
                    text-shadow: 2px 2px 1px #de4040;
                    color: rgba(239,241,234,0.99);
                    line-height; 1.5;
                }

                h4 {
                    color: #e10092;
                    font-family: 楷体
                    font-size: 20px;
                    text-align: center;
                }

                td img {
                    /*width: 60px;*/
                    max-width: 300px;
                    max-height: 300px;
                }

            </style>

        </head>
        """

    # 构造正文表格
    body = \
        """
         <body>
         <div align="center" class="header">
            <!--标题部分的信息-->
            <p align="left">{table_title}</p>
         </div>

         <div class="content">
            <h4></h4>
                <p>    {df_html}
                </p>
        </div>
        </body>
        <br /><br />
        """.format(df_html=df.to_html(index=False), table_title=table_title)

    html_msg = "<html>" + head + body + "</html>"
    if merge_columns_list:
        for r in range(len(merge_columns_list)):
            span_num_list = get_span_num_list(df[merge_columns_list[r]].values.tolist())
            #修正
            if r == 1:
                span_num_list0 = get_span_num_list(df[merge_columns_list[0]].values.tolist())
                for i in range(len(span_num_list)):
                    if sum(span_num_list[:i]) < span_num_list0[0] and sum(span_num_list[:i + 1]) > span_num_list0[0]:
                        a = span_num_list0[0] - sum(span_num_list[:i])
                        b = span_num_list[i] - a
                        span_num_list[i] = a
                        span_num_list.insert(i,b)
                        break
            html_msg = html_merge_cells(html_msg, span_num_list,r + 1)
    return html_msg

#合并单元格
def html_merge_cells(html, span_num_list,col_no):
    html_list = html.split('\n')
    tr_count = 0
    for i in range(len(html_list)):
        if html_list[i].strip() == '<tr>':
            tr_count += 1
            if tr_count == 1:
                html_list[i+col_no] = html_list[i+col_no].replace('<td>','<td rowspan="%s">' % span_num_list[0])
            else:
                found = 0
                for s in range(len(span_num_list)):
                    if tr_count == 1 + sum(span_num_list[:s+1]):
                        html_list[i + col_no] = html_list[i + col_no].replace('<td>', '<td rowspan="%s">' % span_num_list[s+1])
                        found=1
                        break
                if found == 0:
                    html_list[i + col_no] = ''
    #html_result = [i for i in html_list if i != '']
    html_result = '\n'.join(html_list)
    return html_result

def create_result_pic(today, last_week, last_last_week, two_mon_before):
    average_sql = """
	select 
		view.positionid,click.c as click,view.s as show,click.c/(view.s+1) as ctr 
	from 
		(select positionid,count(*) as s from dw.bdl_bd_online_ad_client_show_log where ymd>='{last_week}' and ymd < '{today}' and positionid in ("SP","SSJSYYYW") and lower(productname) like '%mymoney%' group by positionid) view 
	left join 
		(select positionid ,count(*) as c from dw.bdl_bd_onlinead_clicklog where ymd>='{last_week}' and ymd < '{today}' and positionid in ("SP","SSJSYYYW") and lower(productname) like '%mymoney%' group by positionid) click on view.positionid=click.positionid""".format(last_week=last_week, today=today)
    last_average_sql = """
        select
                view.positionid,click.c as click,view.s as show,click.c/(view.s+1) as ctr
        from
                (select positionid,count(*) as s from dw.bdl_bd_online_ad_client_show_log where ymd>='{last_last_week}' and ymd < '{last_week}' and positionid in ("SP","SSJSYYYW") and lower(productname) like '%mymoney%' group by positionid) view 
        left join
                (select positionid ,count(*) as c from dw.bdl_bd_onlinead_clicklog where ymd>='{last_last_week}' and ymd < '{last_week}' and positionid in ("SP","SSJSYYYW") and lower(productname) like '%mymoney%' group by positionid) click on view.positionid=click.positionid""".format(last_last_week=last_last_week, last_week=last_week)


    average_df = spark.sql(average_sql).toPandas()
    last_average_df = spark.sql(last_average_sql).toPandas()
    total_busitype_df = spark.sql("select * from temp.ssj_busitype_table").toPandas()
    busitype_df = total_busitype_df[total_busitype_df['ymd'] >= last_week]
    last_busitype_df = total_busitype_df[(total_busitype_df['ymd'] >= last_last_week) & (total_busitype_df['ymd'] < last_week)]
    two_mon_busitype_df = total_busitype_df[total_busitype_df['ymd'] < last_week]
    ctr_df = busitype_df.groupby(('ymd', 'positionid', 'groupname'), as_index=False).sum()
    ctr_df['ctr'] = ctr_df['click'] / ctr_df['show']
    last_ctr_df = last_busitype_df.groupby(('ymd', 'positionid', 'groupname'), as_index=False).sum()
    last_ctr_df['ctr'] = last_ctr_df['click'] / ctr_df['show']
    outer_data_df = spark.sql("select * from temp.ssj_outer_ad_table_v1").toPandas()
    last_outer_data_df = spark.sql("select * from temp.ssj_outer_ad_table_v2").toPandas()

    total_tail_data = busitype_df
    return ctr_df, last_ctr_df, average_df, last_average_df, busitype_df, last_busitype_df, two_mon_busitype_df, total_tail_data, outer_data_df, last_outer_data_df

def create_plot_df(df, group_column, sp_line, syyyw_line):
    if 'ymd' not in df.columns:
        df['ymd'] = [' 近两月平均'] * len(df)
        df['show'] = df['show'] / 60
        df['click'] = df['click'] / 60
    X = sorted(df['ymd'].unique())
    df['ctr'] = df['click'] / df['show']
    sp_line_Y_list, sp_block_Y_list = [], []
    syyyw_line_Y_list, syyyw_block_Y_list = [], []
    for l in sp_line:
        li = df[(df['positionid'] == 'SP') & (df[group_column] == l)]
        if not len(li):
            li['ctr'] = np.array([0,0,0,0,0,0,0])
            li['show'] = np.array([0,0,0,0,0,0,0])
        sp_line_Y_list.append(li['ctr'].values)
        sp_block_Y_list.append(li['show'].values)
    for l in syyyw_line:
        li = df[(df['positionid'] == 'SSJSYYYW') & (df[group_column] == l)]
        syyyw_line_Y_list.append(li['ctr'].values)
        syyyw_block_Y_list.append(li['show'].values)

    df_sp_line = pd.DataFrame()
    df_sp_block = pd.DataFrame()
    df_syyyw_line = pd.DataFrame()
    df_syyyw_block = pd.DataFrame()

    for i in range(len(sp_line)):
        df_sp_line[sp_line[i]] = sp_line_Y_list[i]
    for j in range(len(syyyw_line)):
        if not all(syyyw_line_Y_list[j] == syyyw_line_Y_list[j]):
            df_syyyw_line[syyyw_line[j]] = np.array([0,0,0,0,0,0,0])
            continue
        df_syyyw_line[syyyw_line[j]] = syyyw_line_Y_list[j]
    df_sp_line.index = X
    df_syyyw_line.index = X
    df_sp_block = df[(df[group_column].isin(sp_line)) & (df['positionid'] == 'SP')][['ymd',group_column,'show']]
    df_syyyw_block = df[(df[group_column].isin(syyyw_line)) & (df['positionid'] == 'SSJSYYYW')][['ymd',group_column,'show']]
    return df_sp_line, df_syyyw_line, df_sp_block, df_syyyw_block


def ser_plot(df_line, df_block, save_path, group_column):
    plt.clf()
    sns.plotting_context({'axes.titlesize': 12.0,'legend.fontsize': 0.5,'lines.linewidth': 0.75})
    plt.figure(figsize=(14,4))
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)
    avg_sort_df = pd.merge(df_block,df_block.groupby(group_column,as_index=False).mean()[[group_column,'show']].rename(columns={'show':'avg_show'}),on=[group_column],how='left')
    df_block = avg_sort_df.sort_values(['ymd','avg_show'],ascending=[True,False]).drop(columns=['avg_show'],axis=1)
    ax = sns.barplot(x='ymd',y='show',hue=group_column, data=df_block,errwidth=0)
    plt.legend(loc='best',fontsize=8)
    plt.ylabel("曝光量")
    plt.xlabel("")
    sns.set(font='SimHei',style='white')
    ax2 = ax.twinx()
    if group_column != 'account_name':
        sns.lineplot(data=df_line,dashes=False,marker='o',palette = ['green','red'])
    else:
        sns.lineplot(data=df_line,dashes=False,marker='o',style='darkgrid')
    plt.legend(loc=9, bbox_to_anchor=(0.5, 1.165), ncol=5,fontsize=9.2)  #设置图例位置和格式
    plt.ylabel("点击率")
    plt.xlabel("")
    plt.savefig(save_path, format='png')

if __name__ == '__main__':
    #获取数据
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    last_week = datetime.datetime.strftime((datetime.datetime.strptime(today,'%Y-%m-%d')-datetime.timedelta(days=7)),'%Y-%m-%d')
    last_last_week = datetime.datetime.strftime((datetime.datetime.strptime(last_week,'%Y-%m-%d')-datetime.timedelta(days=7)),'%Y-%m-%d')
    two_mon_before = datetime.datetime.strftime((datetime.datetime.strptime(last_week,'%Y-%m-%d')-datetime.timedelta(days=60)),'%Y-%m-%d')
    ctr_df, last_ctr_df, average_df, last_average_df, busitype_df, last_busitype_df, two_mon_busitype_df, total_tail_data, outer_data_df, last_outer_data_df = create_result_pic(today,last_week,last_last_week,two_mon_before)

    #实验组信息
    sp_line = ['NO_TCTR_Group_BIGDATA','CTR_GBDT_Model_DJ','SSJ_CVR_MODEL_DJ','SSJ_WDL_SYYYW_DJ','SSJ_WDL_DJ','SSJ_WDL_SP_DJ','SSJ_DEEPFM_V1_DJ']
    syyyw_line = ['NO_TCTR_Group_BIGDATA','CTR_GBDT_Model_DJ','SSJ_CVR_MODEL_DJ','SSJ_WDL_SYYYW_DJ','SSJ_WDL_DJ','SSJ_WDL_V2_DJ']
    line_describe_dict = {'NO_TCTR_Group_BIGDATA':'随机组','CTR_GBDT_Model_DJ':'新手用户流量','SSJ_CVR_MODEL_DJ':'转化率模型','SSJ_WDL_SYYYW_DJ':'WDL baseline','SSJ_WDL_DJ':'WDL 实验组','SSJ_WDL_SP_DJ':'WDL 实验组','SSJ_DEEPFM_V1_DJ':'deepFM实验模型','SSJ_WDL_V2_DJ':'WDL 实验组'}
    sp_rate_dict = {'NO_TCTR_Group_BIGDATA':'10%','CTR_GBDT_Model_DJ':'-','SSJ_CVR_MODEL_DJ':'15%','SSJ_WDL_SYYYW_DJ':'60%','SSJ_WDL_DJ':'5%','SSJ_WDL_SP_DJ':'5%','SSJ_DEEPFM_V1_DJ':'5%'}
    syyyw_rate_dict = {'NO_TCTR_Group_BIGDATA':'10%','CTR_GBDT_Model_DJ':'-','SSJ_CVR_MODEL_DJ':'15%','SSJ_WDL_SYYYW_DJ':'65%','SSJ_WDL_DJ':'5%','SSJ_WDL_V2_DJ':'5%'}

    #点击信息表格生成
    table_result = []
    last_ctr_df['ymd'] = [datetime.datetime.strftime((datetime.datetime.strptime(i,'%Y-%m-%d')+datetime.timedelta(days=7)),'%Y-%m-%d') for i in last_ctr_df['ymd']]
    merge_df = ctr_df.merge(last_ctr_df, how = 'left', on = ['ymd','positionid','groupname'])
    last_busitype_df['ymd'] = [datetime.datetime.strftime((datetime.datetime.strptime(i,'%Y-%m-%d')+datetime.timedelta(days=7)),'%Y-%m-%d') for i in last_busitype_df['ymd']]
    merge_busitype_df = busitype_df.merge(last_busitype_df, how='left', on=['ymd','positionid','systemname','account_name','groupname'])
        
    for p in ['SP','SSJSYYYW']:
        describe, rate= [], []
        rate_dict = sp_rate_dict
        if p == 'SSJSYYYW':
            rate_dict = syyyw_rate_dict
        df_group = merge_df[merge_df['positionid']==p][['show_x','click_x','show_y','click_y','groupname']].groupby('groupname',as_index=False).sum()
        df_group = df_group[df_group['groupname'].isin(line_describe_dict.keys())]
        for l in df_group['groupname'].values:
            describe.append(line_describe_dict[l])
            rate.append(rate_dict[l])
        df_group['describe'] = describe
        df_group['rate'] = rate
        df_group = df_group.rename(columns={'groupname':'实验线','describe':'说明','rate':'流量占比','show_x':'曝光量','click_x':'点击量'})
        df_group['点击率'] = ['%.2f%%' % (d * 100) for d in df_group['点击量'] / df_group['曝光量']]
        df_group['点击占比'] = ['%.2f%%' % (d * 100) for d in df_group['点击量'] / df_group['点击量'].sum()]
        df_group['曝光占比'] = ['%.2f%%' % (d * 100) for d in df_group['曝光量'] / df_group['曝光量'].sum()]
        df_group['曝光量环比上上周'] = ['%.2f%%' % (i*100) for i in (df_group['曝光量'] - df_group['show_y'])/df_group['show_y']]
        df_group['点击量环比上上周'] = ['%.2f%%' % (i*100) for i in (df_group['点击量'] - df_group['click_y'])/df_group['click_y']]
        df_group['点击率环比上上周'] = ['%.2f%%' % (i*100) for i in (df_group['点击量'] / df_group['曝光量'] - df_group['click_y']/df_group['show_y'])/(df_group['click_y']/df_group['show_y'])]
        df_group = df_group.drop(columns=['show_y','click_y'],axis=1)
        df_group = df_group[['实验线','说明','流量占比','曝光量','曝光占比','点击量','点击占比','点击率','曝光量环比上上周','点击量环比上上周','点击率环比上上周']].reset_index(drop=True)
        df_group = df_group.sort_values(['曝光量'],ascending = False)
        concat_table = []
        for sys in ['Android','ios']:
            busitype_table = merge_busitype_df[(merge_busitype_df['positionid'] == p) & (merge_busitype_df['systemname'] == sys)].groupby('account_name',as_index=False).sum()[['account_name','show_x','click_x','show_y','click_y']].rename(columns={'account_name':'业务线','show_x':'曝光量','click_x':'点击量'})
            busitype_table['点击率'] = ['%.2f%%' % (d * 100) for d in busitype_table['点击量']/busitype_table['曝光量']]
            busitype_table['曝光占比'] = ['%.2f%%' % (d * 100) for d in busitype_table['曝光量'] / busitype_table['曝光量'].sum()]
            busitype_table['操作系统'] = [sys] * len(busitype_table)
            busitype_table = busitype_table[['操作系统','业务线','曝光量','曝光占比','点击量','点击率','show_y','click_y']]
            busitype_table['曝光量环比上上周'] = ['%.2f%%' % (i*100) for i in (busitype_table['曝光量'] - busitype_table['show_y'])/busitype_table['show_y']]
            busitype_table['点击量环比上上周'] = ['%.2f%%' % (i*100) for i in (busitype_table['点击量'] - busitype_table['click_y'])/busitype_table['click_y']]
            busitype_table['点击率环比上上周'] = ['%.2f%%' % (i*100) for i in (busitype_table['点击量']/busitype_table['曝光量'] - busitype_table['click_y']/busitype_table['show_y'])/(busitype_table['click_y']/busitype_table['show_y'])]
            busitype_table = busitype_table.drop(columns=['show_y','click_y'],axis=1)
            busitype_table = busitype_table.sort_values(['曝光量'],ascending = False)
            concat_table.append(busitype_table)
        table_result += [concat_table[0].append(concat_table[1]), df_group]

    sp_busitype_table_html = get_html_msg(table_result[0], "<b>随手记闪屏上周点击率统计（分操作系统和业务线）：</b>", ['操作系统'])
    sp_table_html = get_html_msg(table_result[1], "<b>随手记闪屏上周点击率统计（分实验线）：</b>", [])
    syyyw_busitype_table_html = get_html_msg(table_result[2], "<b>随手记首页运营位上周点击率统计（分操作系统和业务线）：</b>", ['操作系统'])
    syyyw_table_html = get_html_msg(table_result[3], "<b>随手记首页运营位上周点击率统计（分实验线）：</b>", [])

    #其他运营位表格
    other_position_df = merge_busitype_df[merge_busitype_df['positionid'].isin([i for i in list(merge_busitype_df['positionid'].unique()) if i not in ('SP','SSJSYYYW')])]
    other_position_df = other_position_df.groupby(('positionid','systemname','account_name'),as_index=False).sum()[['positionid','systemname','account_name','show_x','click_x','show_y','click_y']].rename(columns={'positionid':'运营位','systemname':'操作系统','account_name':'业务线','show_x':'曝光量','click_x':'点击量'})
    other_position_df['点击率']= ['%.2f%%' % (d * 100) for d in other_position_df['点击量']/other_position_df['曝光量']]
    other_position_df['曝光占比']= ['%.2f%%' % (d * 100) for d in other_position_df['曝光量']/other_position_df['曝光量'].sum()]
    other_position_df = other_position_df[['运营位','操作系统','业务线','曝光量','曝光占比','点击量','点击率','show_y','click_y']]
    other_position_df['曝光量环比上上周'] = ['%.2f%%' % (i*100) for i in (other_position_df['曝光量'] - other_position_df['show_y'])/other_position_df['show_y']]
    other_position_df['点击量环比上上周'] = ['%.2f%%' % (i*100) for i in (other_position_df['点击量'] - other_position_df['click_y'])/other_position_df['click_y']]
    other_position_df['点击率环比上上周'] = ['%.2f%%' % (i*100) for i in (other_position_df['点击量']/other_position_df['曝光量'] - other_position_df['click_y']/other_position_df['show_y'])/(other_position_df['click_y']/other_position_df['show_y'])]
    other_position_df = other_position_df.drop(columns=['show_y','click_y'],axis=1)
    other_position_df = pd.merge(other_position_df,other_position_df.groupby('运营位',as_index=False).sum()[['运营位','曝光量']].rename(columns={'曝光量': 'avg_show'}), on=['运营位'], how='left')
    other_position_df = other_position_df.sort_values(['avg_show','操作系统','曝光量'],ascending = [False,True,False]).drop(columns=['avg_show'],axis=1)
    other_position_df['运营位'] = other_position_df['运营位'].map(lambda x:POSITIONID_DICT[x] if x in POSITIONID_DICT.keys() else x)
    print ("other_position_df: ")
    print (other_position_df)
    other_position_table = get_html_msg(other_position_df, "<b>随手记其他运营位上周点击率统计（分操作系统和业务线）：</b>", ['运营位'])
          
    #外部广告数据表格
    last_outer_data_df['ymd'] = [datetime.datetime.strftime((datetime.datetime.strptime(i, '%Y-%m-%d') + datetime.timedelta(days=7)),'%Y-%m-%d') for i in last_outer_data_df['ymd']]
    merge_outer_df = outer_data_df.merge(last_outer_data_df, how='left', on=['ymd','group_name','position_id','system_name'])
    merge_outer_df = merge_outer_df.groupby(('group_name','position_id','system_name'),as_index=False).sum()[['group_name','position_id','system_name','show_x','click_x','show_y','click_y']].rename(columns={'position_id':'运营位','system_name':'操作系统','group_name':'接入平台','show_x':'曝光量','click_x':'点击量'})
    merge_outer_df['点击率'] = ['%.2f%%' % (d * 100) for d in merge_outer_df['点击量']/merge_outer_df['曝光量']]
    merge_outer_df['曝光占比']= ['%.2f%%' % (d * 100) for d in merge_outer_df['曝光量']/merge_outer_df['曝光量'].sum()]
    merge_outer_df = merge_outer_df[['接入平台','运营位','操作系统','曝光量','曝光占比','点击量','点击率','show_y','click_y']]
    merge_outer_df['曝光量环比上上周'] = ['%.2f%%' % (i*100) for i in (merge_outer_df['曝光量'] - merge_outer_df['show_y'])/merge_outer_df['show_y']]
    merge_outer_df['点击量环比上上周'] = ['%.2f%%' % (i*100) for i in (merge_outer_df['点击量'] - merge_outer_df['click_y'])/merge_outer_df['click_y']]
    merge_outer_df['点击率环比上上周'] = ['%.2f%%' % (i*100) for i in (merge_outer_df['点击量']/merge_outer_df['曝光量'] - merge_outer_df['click_y']/merge_outer_df['show_y'])/(merge_outer_df['click_y']/merge_outer_df['show_y'])]
    merge_outer_df = merge_outer_df.drop(columns=['show_y','click_y'],axis=1)
    merge_outer_df = pd.merge(merge_outer_df,merge_outer_df.groupby(('接入平台','运营位'),as_index=False).sum()[['接入平台','运营位','曝光量']].rename(columns={'曝光量': 'avg_show'}), on=['接入平台','运营位'], how='left')
    merge_outer_df = merge_outer_df.groupby('接入平台',sort=True).apply(lambda x:x.sort_values(['avg_show','操作系统','曝光量'],ascending = [False,True,False])).drop(columns=['avg_show'],axis=1)
    merge_outer_df['运营位'] = merge_outer_df['运营位'].map(lambda x: POSITIONID_DICT[x] if x in POSITIONID_DICT.keys() else x)
    merge_outer_df['接入平台'] = merge_outer_df['接入平台'].map(lambda x: POSITIONID_DICT[x] if x in POSITIONID_DICT.keys() else x)
    merge_outer_table = get_html_msg(merge_outer_df , "<b>随手记外部广告上周点击率统计（分平台、运营位和操作系统）：</b>", ['接入平台','运营位'])


    #画图
    df_sp_line, df_syyyw_line, df_sp_block, df_syyyw_block = create_plot_df(ctr_df, 'groupname', sp_line, syyyw_line)
    sp_busitype_line = ['理财-随手记','生活服务-随手记','办卡-随手记','产品功能-随手记','账本市场-随手记','保险-随手记','产品运营-随手记']
    syyyw_busitype_line = ['理财-随手记','生活服务-随手记','办卡-随手记','产品功能-随手记','产品运营-随手记']
    system_line = ['Android','ios']
    df_busitype_sp_line, df_busitype_syyyw_line, df_busitype_sp_block, df_busitype_syyyw_block = create_plot_df(busitype_df.groupby(('ymd','positionid','account_name'),as_index=False).sum(), 'account_name', sp_busitype_line, syyyw_busitype_line)
    df_system_sp_line, df_system_syyyw_line, df_system_sp_block, df_system_syyyw_block = create_plot_df(busitype_df.groupby(('ymd','positionid','systemname'),as_index=False).sum(), 'systemname', system_line, system_line)
    df_avg_sp_line, df_avg_syyyw_line, df_avg_sp_block, df_avg_syyyw_block = create_plot_df(two_mon_busitype_df.groupby(('positionid','account_name'),as_index=False).sum(), 'account_name', sp_busitype_line, syyyw_busitype_line)
    df_avg_s_sp_line, df_avg_s_syyyw_line, df_avg_s_sp_block, df_avg_s_syyyw_block = create_plot_df(two_mon_busitype_df.groupby(('positionid','systemname'),as_index=False).sum(), 'systemname', system_line, system_line)

    path = '/home/tico/zhijieliao/ssj_weekly_report/'
    sns.set(font='SimHei',style='darkgrid')
    ser_plot(df_busitype_sp_line.append(df_avg_sp_line),df_busitype_sp_block.append(df_avg_sp_block), path + 'sp_busitype_ctr.png','account_name')
    sns.set(font='SimHei',style='darkgrid')
    ser_plot(df_busitype_syyyw_line.append(df_avg_syyyw_line), df_busitype_syyyw_block.append(df_avg_syyyw_block),path + 'syyyw_busitype_ctr.png','account_name')
    sns.set(font='SimHei',style='darkgrid')
    ser_plot(df_system_sp_line.append(df_avg_s_sp_line),df_system_sp_block.append(df_avg_s_sp_block), path + 'sp_system_ctr.png','systemname')
    sns.set(font='SimHei',style='darkgrid')
    ser_plot(df_system_syyyw_line.append(df_avg_s_syyyw_line), df_system_syyyw_block.append(df_avg_s_syyyw_block), path + 'syyyw_system_ctr.png','systemname')
        
    #环比
    last_sp_ctr = average_df[average_df['positionid']=='SP']['ctr'].values[0]
    last_syyyw_ctr = average_df[average_df['positionid']=='SSJSYYYW']['ctr'].values[0]
    last_last_sp_ctr = last_average_df[last_average_df['positionid']=='SP']['ctr'].values[0]
    last_last_syyyw_ctr = last_average_df[last_average_df['positionid']=='SSJSYYYW']['ctr'].values[0]
    last_sp_show = average_df[average_df['positionid']=='SP']['show'].values[0]
    last_syyyw_show = average_df[average_df['positionid']=='SSJSYYYW']['show'].values[0]
    last_last_sp_show = last_average_df[last_average_df['positionid']=='SP']['show'].values[0]
    last_last_syyyw_show = last_average_df[last_average_df['positionid']=='SSJSYYYW']['show'].values[0]

    sp_ctr_flow_rate = '%.2f%%' % ((last_sp_ctr - last_last_sp_ctr)*100 / last_last_sp_ctr)
    sp_show_flow_rate = '%.2f%%' % ((last_sp_show - last_last_sp_show)*100 / last_last_sp_show)
    syyyw_ctr_flow_rate = '%.2f%%' % ((last_syyyw_ctr - last_last_syyyw_ctr)*100 / last_last_syyyw_ctr)
    syyyw_show_flow_rate = '%.2f%%' % ((last_syyyw_show - last_last_syyyw_show)*100 / last_last_syyyw_show)
       
    #邮件元素汇总
    sp_word = "<h2>一.随手记闪屏：</h2><br>" + "\t--上上周闪屏平均点击率: %s, 上周闪屏平均点击率: %s, 环比%s%s" % ('%.2f%%' % (last_last_sp_ctr*100), '%.2f%%' % (last_sp_ctr*100), "上升" if '-' not in sp_ctr_flow_rate else "下降", sp_ctr_flow_rate.replace('-','')) + "<br>" + "\t--上上周闪屏总曝光量: %d, 上周闪屏总曝光量: %d, 环比%s%s" % (last_last_sp_show, last_sp_show, "上升" if not '-' in sp_show_flow_rate else "下降", sp_show_flow_rate.replace('-','')) + "<br><br>"
    syyyw_word = "<br><br><h2>二.随手记首页运营位：</h2><br>" + "\t--上上周首页运营位平均点击率: %s, 上周首页运营位平均点击率: %s, 环比%s%s" % ('%.2f%%' % (last_last_syyyw_ctr*100), '%.2f%%' % (last_syyyw_ctr*100), "上升" if '-' not in syyyw_ctr_flow_rate else "下降", syyyw_ctr_flow_rate.replace('-','')) + "<br>" + "\t--上上周首页运营位总曝光量: %d, 上周首页运营位总曝光量: %d, 环比%s%s" % (last_last_syyyw_show, last_syyyw_show, "上升" if '-' not in syyyw_show_flow_rate else "下降", syyyw_show_flow_rate.replace('-','')) +  "<br><br>"
    other_word = "<br><br><br><h2>三.随手记其他运营位：</h2><br><br>"
    outer_word = "<br><br><h2>四.随手记外部广告上周各运营位统计：</h2><br><br>"
    head_msg = """
                Hi，早上好~<br>
                以下是随手记闪屏和首页运营位上周（%s ~ %s）的周报：<br><br><br>""" % (last_week, today)
    tail_msg = """
                <br><br><br><br><br>----------------------------------------------------<br>
                <b>廖志杰<br>
                智能算法部-算法工程师<br>
                邮箱：zhijie_liao@sui.com</b>
                """
    picture_path_list = [path + 'sp_busitype_ctr.png', path + 'syyyw_busitype_ctr.png', path + 'sp_system_ctr.png', path + 'syyyw_system_ctr.png']
    html_table_list = [sp_busitype_table_html,sp_table_html,syyyw_busitype_table_html,syyyw_table_html,other_position_table,merge_outer_table]
    summary_word = [sp_word,syyyw_word,other_word,outer_word]
    mix_content, picture_attach_list = email_content_mix(picture_path_list, html_table_list, head_msg, tail_msg, summary_word)
    avg_two_mon = two_mon_busitype_df.groupby(['positionid','systemname','account_name','groupname'],as_index=False).mean()
    avg_two_mon['ymd'] = ['近两月平均'] * len(avg_two_mon)
    avg_two_mon['ctr'] = avg_two_mon['click'] / avg_two_mon['show']
    total_tail_data = avg_two_mon.append(total_tail_data)
    total_tail_data = total_tail_data[['ymd','positionid','systemname','account_name','groupname','show','click','ctr']].rename(columns={'ymd':'日期','positionid':'运营位','systemname':'操作系统','account_name':'业务线','groupname':'实验线','show':'曝光量','click':'点击量','ctr':'点击率'})
    total_tail_data['运营位'] = total_tail_data['运营位'].map(lambda x: POSITIONID_DICT[x] if x in POSITIONID_DICT.keys() else x)
    total_tail_data.to_excel(path + 'total_tail_data.xlsx', index = False)

    send_email("随手记闪屏&首页运营位 CTR周报", mix_content, picture_attach_list, path + 'total_tail_data.xlsx')





'''
<h1> ~ <h6>  6种标题
<b>  加粗字体
<br>  换行
'''
