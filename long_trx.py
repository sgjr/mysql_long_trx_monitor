#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: pinocao
# crate_date: 2019-04-22
# update_data: 2019-04-26
"""
此脚本主要用来监控MySQL主库的长事务，
通过读取mysql.cfg配置文件，获取MySQL连接地址，以及告警阈值
日志按天记录至当前文件夹下边的log目录中，日志格式为YYYY-mm-dd.log
通过pushgateway进行打点
"""

import os
import re
import json
import configparser
import pymysql
import urllib.request
import ssl
import datetime
import logging
import prometheus_client
from prometheus_client import Gauge
from prometheus_client.core import CollectorRegistry
import requests
ssl._create_default_https_context = ssl._create_unverified_context


# pushgateway配置
REGISTRY1 = CollectorRegistry(auto_describe=False)
mysql_longtrx_count = Gauge('mysql_longtrx_count', 'record count of mysql long transactions', \
                                 ['dbhost', 'dbport'], registry=REGISTRY1)


dingUrl = ""    #钉钉地址
          "=97ff1a777e40f163a95d4946d8491b1ccd4383b67113ce2cb2fefb41de4503e7"    # 钉钉地址
cfgfile = os.getcwd() + '/mysql.cfg'    # 配置文件地址
tolist = ''    # 艾特人员
stype = 'text'    # 报警文本格式
mon_user = 'monitor_user'    # MySQL监控用户
mon_pass = '123456'    # MySQL监控用户密码
joburl = 'http://127.0.0.1:50004/metrics/job/'    # pushgateway地址




# push MySQL长事务个数
def longTrxCount(exported_job ,dbhost, dbport, totalcount):
    mysql_longtrx_count.labels(dbhost=dbhost, dbport=dbport).set(totalcount)
    requests.post(joburl + exported_job, data=prometheus_client.generate_latest(REGISTRY1))


# 日志记录
def create_logger():
    # 判断日志文件夹是否存在
    logdir = os.getcwd() + '/log'
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    # 指定日志文件名
    logfile = os.getcwd() + '/log/' + datetime.datetime.now().strftime('%Y%m%d') + '.log'

    # 日志配置
    fh = logging.FileHandler(filename=logfile, encoding='utf-8', mode='a')
    logging.basicConfig(level=logging.DEBUG,
                    handlers = [fh],
                    format='%(asctime)s [%(levelname)s] \n%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('monitor_long_trx')
    return logger


logger = create_logger()


#钉钉提醒
'''
url为钉钉webhook的地址
tolist为需要@的人员的钉钉号，''为不艾特
msg需要发送的消息
stype为发送消息的格式，text格式或者markdown格式
'''


def send_msg(url, tolist, msg, stype):
    #       构建请求头部
    header = {
        "Content-Type": "application/json",
        "Charset": "utf-8"
    }
    if stype == 'text':
        if tolist:
            tolist = re.split("[,; ]", tolist)
        else:
            tolist = ''
    #       构建请求数据
        data = {
            "msgtype": "text",
            "text": {
                "content": msg
            },

            "at": {
                "atMobiles": tolist,
                "isAtAll": False
            }
        }
    elif stype == 'markdown':
        if tolist:
            tolist = re.split("[,; ]", tolist)
            tolist_str = '@' + '@'.join(tolist)
        else:
            tolist = ''
            tolist_str = ''
        # 构建请求数据
        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": "钉钉提醒",
                "text": msg + tolist_str
            },
            "at": {
                "atMobiles": tolist,
                "isAtAll": False
            }
        }
    else:
        logger.error('Error format of messages !!!')
    # 对请求数据进行json封装，转换为json格式
    senddata = json.dumps(data).encode(encoding="utf-8")

    try:
        # 发送请求
        request = urllib.request.Request(url=url, data=senddata, headers=header)
        # 将返回的请求构建成文件格式
        opener = urllib.request.urlopen(request)
        return True
    except Exception as errormsg:
        logger.error(errormsg)
        return False


def executesql(dbhost, dbport, sql):
    threadIdList = []
    threadUserList = []
    threadHostList = []
    threadDbList = []
    threadSqlList = []
    try:
        conn = pymysql.connect(host=dbhost, port=int(dbport), user=mon_user, password=mon_pass, charset='utf8')
        cur = conn.cursor()
    except Exception as e:
        logger.error(e)
        return False
    try:
        cur.execute(sql)
        results = cur.fetchall()
        total = len(results)
        longTrxCount('mysql_transactions', dbhost, dbport, total)
        logger.info('Push successfully!')
        if total > 0:
            for result in results:
                threadIdList.append(result[1])
                threadUserList.append(result[2])
                threadHostList.append(result[3])
                threadDbList.append(result[4])
                if result[6] is None:
                    threadSqlList.append('NULL')
                else:
                    threadSqlList.append(result[6])
            message = 'MySQL server: ' + dbhost + ':' + dbport + '\n' + '线程ID: ' + str(threadIdList) + '\n' + '连接用户: ' + str(threadUserList) + '\n' + '连接地址: ' + \
                      str(threadHostList) + '\n' + '连接DB: ' + str(threadDbList) + '\n' + '长事务合计: ' + str(total) + ' 个'
            logger.info(message + '\n' + '\n'.join(threadSqlList))
            send_msg(dingUrl, tolist, message, stype)
    except Exception as e:
        logger.error(e)
    finally:
        conn.close()


def main():
    config = configparser.ConfigParser()
    config.read(cfgfile)
    DbServers = config.sections()
    for dbserver in DbServers:
        dbhost = config[dbserver]['host']
        dbport = config[dbserver]['port']
        continue_time = config[dbserver]['continue_time']
        currentTime = (datetime.datetime.now() - datetime.timedelta(seconds=int(continue_time))).strftime("%Y-%m-%d %H:%M:%S")
        sql = '''select t1.trx_started,t1.trx_mysql_thread_id,t2.USER,t2.HOST,t2.DB,t2.TIME,t2.INFO from \
        information_schema.INNODB_TRX t1 join information_schema.processlist t2 \
        on t1.trx_mysql_thread_id=t2.id where t1.trx_started <= '%s';''' % currentTime
        executesql(dbhost, dbport, sql)



main()


