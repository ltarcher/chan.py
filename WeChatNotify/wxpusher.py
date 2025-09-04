#!/usr/bin/env python
# -*- encoding=utf8 -*-

from math import ceil
import os
import json
import requests

'''
WxPusher是免费的推送服务，为了能更好的服务大家，这里说明一下系统相关数据限制

消息发送，必须合法合规，发送违规违法欺诈等等非正常消息，可能被封号；
WxPusher推送的是实时消息，时效性比较强，过期以后消息也就没有价值了，目前WxPusher会为你保留7天的数据 ，7天以后不再提供可靠性保证，会不定时清理历史消息；
单条消息的数据长度(字符数)限制是：content<40000;summary<100;url<400;
单条消息最大发送UID的数量<2000，单条消息最大发送topicIds的数量<5;
单个微信用户，也就是单个UID，每天最多接收500条消息，请合理安排发送频率。
'''

http_headers={
            'User-Agent':'Mozilla/4.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
            'Content-Type':'application/json',
        }

class Wxpusher(object):
    def __init__(self):
        self.config_file = "config.json"
        self.users = []
        if os.path.exists(self.config_file):
            self.config = self.loadConfig(self.config_file)
            self.appToken = self.config["appToken"]
        else:
            raise Exception("配置文件不存在")

    #读取配置
    def loadConfig(self, filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)

    #保存配置
    def saveConfig(self, filename, config):
        content = json.dumps(config, sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False)
    
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)

    #Get接口
    def get_url_content(self, urls, params, http_header = http_headers):
        result = requests.get(urls, params=params,headers=http_header, verify=False)
        if result.status_code == 200:
            if len(result.content):
                return result.content.decode("utf-8")
            else:
                return "{}"
        else:
            return "{}"

    #Put接口
    def put_url_content(self, urls, params, http_header = http_headers):
        result = requests.put(urls, params=params, headers=http_header, verify=False)
        if result.status_code == 200:
            if len(result.content):
                return result.content.decode("utf-8")
            else:
                return "{}"
        else:
            return "{}"

    #Post接口
    def post_url_content(self, urls, params, http_header = http_headers):

        result = requests.post(urls, json=params, headers=http_header, verify=False)
        if result.status_code == 200:
            if len(result.content):
                return result.content.decode("utf-8")
            else:
                return "{}"
        else:
            return "{}"

    #获取关注的所有用户列表
    def get_users(self):
        users_list = []
        url = "https://wxpusher.zjiecode.com/api/fun/wxuser/v2"
        page = 1
        params = {
                "appToken": self.appToken,
                "page": page, 
                "pageSize": 100,
        }
        while True:
            users = json.loads(self.get_url_content(url, params))
            if not users['success']:
                raise Exception("获取用户列表失败")
            
            if not "data" in users or not users["data"]:
                #没有data数据
                return

            if "records" in users['data']:
                users_list = users_list + users['data']['records']

            total = users["data"]["total"]
            current_page = users["data"]["page"]
            pageSize = users["data"]['pageSize']
            if current_page >= ceil(total / pageSize):
                break
            
            params['page'] = current_page + 1
        
        self.users = users_list

    #保存用户列表到本地
    def saveUsers(self):
        self.saveConfig("users.json", self.users)

    #发送消息到用户微信
    #uids和topicIds至少传一个
    #msg最大40000字
    def wx_send_msg(self, msg, uids=None, title="", topicIds=None, contentType=1, source_url=None):
        req_url = "http://wxpusher.zjiecode.com/api/send/message"
        params = {
            "appToken": self.appToken,
            "content": msg,
            "summary": title, 
            "contentType": contentType
        }

        if not uids and not topicIds:
            raise Exception("uids或topicIds至少传一个")

        if uids:
            params["uids"] = uids

        if topicIds:
            params["topicIds"] = topicIds

        if source_url:
            params["url"] = source_url

        return self.post_url_content(req_url, params)
    
    #根据主题名或主题id判断是否启用
    def check_topic_enable(self, topic_name=None, topic_id=None):
        for topic in self.config["topics"]:
            if topic_name and topic["name"] == topic_name:
                return topic["enable"]
            if topic_id and topic["id"] == topic_id:
                return topic["enable"]

    #根据主题名或主题id判断是否启用消息发送到微信
    def check_topic_enable_sendwx(self, topic_name=None, topic_id=None):
        for topic in self.config["topics"]:
            if topic_name and topic["name"] == topic_name:
                return topic["sendwx"]
            if topic_id and topic["id"] == topic_id:
                return topic["sendwx"]
            
    #根据主题名称查找主题id
    def get_topicid_by_name(self, topic_name):
        for topic in self.config["topics"]:
            if topic["name"] == topic_name:
                return topic["id"]

    #按照主题给相应关注用户发送消息
    def wx_send_topic_group_msg(self, msg, topicid, title="", contentType=1, source_url=None): 
        uids = []
        for user in self.users:
            #type为1表示关注主题的用户, reject表示被拉黑的用户
            if user["type"] == 1 and not user["reject"] and user["appOrTopicId"] == topicid:
                uids.append(user["uid"])

        #没有用户关注或该主题未启用或未启用发送消息到微信则直接返回
        if len(uids) == 0 or not self.check_topic_enable(topic_id=topicid) or not self.check_topic_enable_sendwx(topic_id=topicid):
            return ""
        
        return self.wx_send_msg(msg, uids=uids, title=title, contentType=contentType, source_url=source_url)
    
    #按照主题名称给相应关注用户发送消息
    def wx_send_topicname_group_msg(self, msg, topicname, title="", contentType=1, source_url=None): 
        topicid = self.get_topicid_by_name(topicname)
        return self.wx_send_topic_group_msg(msg, topicid=topicid, title=title, contentType=contentType, source_url=source_url)
    
