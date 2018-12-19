# -*- coding:utf-8 -*-
import hashlib
import json
import queue
import threading
import traceback
import io
import DatabaseOpt
import DownFile
import time
import mylog
import os
from Resultpost import ResultSend
DownloadDir = "d:\\Project\\easypai\\bin\\"
#DownloadDir = "/home/ubuntu/easypai/bin/templatefile"


class Msg:
    def __init__(self, loger=None):
        self.msg_queue = queue.Queue(100)     # 外部消息队列
        self.dl_msg_queue = queue.Queue(100)  # 下载结果消息队列
        self.loger = mylog.logger
        self.dl = DownFile.FileDownload(self.dl_msg_queue, loger)
        self.request_head = {}
        self.response_desc = {}
        self.h_thread = threading.Thread(target=self.process)
        self.h_dl_thread = threading.Thread(target=self.process_dl_msg)
        self.response_desc[0] = 'success add job queue'
        self.response_desc[1] = 'syntax error'
        self.response_desc[2] = 'no enough permission '
        self.response_desc[3] = 'token error'
        self.response_desc[4] = 'this task not exist'
        self.response_desc[5] = 'this user not exist '
        self.response_desc[6] = 'program inner error '
        self.response_desc[7] = 'template already exist'
        self.response_desc[8] = 'template not exist'
        DatabaseOpt.db.cnn_db()

    def start(self):
        self.h_thread.start()
        self.h_dl_thread.start()
        self.dl.start()

    def process_dl_msg(self):
        while True:
            dl_rst = self.dl_msg_queue.get()  # [id,下载type,下载dl_ret 0成功 -1：失败]
            id = dl_rst[0]
            dl_type = dl_rst[1]
            dl_rst_flag_ls = dl_rst[2]
            if dl_type == 'CreateTemplate':
                resultaddr = self.request_head[id]['ResultURL']
                result_json = self.gen_create_template_result_msg(id, dl_rst_flag_ls[0])
                ResultSend.send(resultaddr, result_json)
            elif dl_type == "MatchImage":
                resultaddr = self.request_head[id]['ResultURL']
                if sum(dl_rst_flag_ls) != 0: # 每个下载都是成功的话dl_rst_flag_ls为0
                    result_json = self.gen_match_image_err_result_msg(id, dl_rst_flag_ls)
                    ResultSend.send(resultaddr, result_json)
            elif dl_type == "MatchVideo":
                pass
            self.dl_msg_queue.task_done()

    def add_task_queue(self, item):
        '''加入消息队列'''
        self.msg_queue.put(item)

    def process(self):
        try:
            while True:
                encode = self.msg_queue.get()
                # print("%s"%encode)
                # encode = json.loads(task_str)
                cmdword = encode["MsgHead"]["Cmd"]
                if cmdword == "CreateTemplate":
                    self.loger.info("process msg (CreateTempate):%s" % encode)
                    self.process_create_template_msg(encode)
                elif cmdword == "DeleteTemplate":
                    self.loger.info("process msg (DeletetTemplate):%s" % encode)
                    self.process_del_template_msg(encode)
                elif cmdword == "MatchImage":
                    self.loger.info("process msg (MatchImage):%s" % encode)
                    self.process_match_image_msg(encode)
                elif cmdword == "MatchVideo":
                    pass

                self.msg_queue.task_done()
        except Exception as e:
            fp = io.StringIO()
            traceback.print_exc(file=fp)
            message = fp.getvalue()
            # print("except:%s"%message)
            self.loger.info(message)

    def process_create_template_msg(self, encode):
        '''处理创建模板消息'''
        if encode["MsgHead"]["Cmd"] != "CreateTemplate":
            self.loger.info("Msg {} can't processed process_create_template_msg!".format(encode))
            return -1
        id = encode["MsgHead"]["MsgID"]  #  暂存消息头时用id作为标识
        dl_url = encode['TemplateURL']
        sql = "insert into Template(TaskID,TemplateID,TemplateName,TemplateURL,CreateDataTime) " \
              "values('{}','{}','{}','{}','{}')".format(id, encode['TemplateID'], encode['TemplateName'],
              dl_url, encode['MsgHead']['DateTime'])
        row = []
        DatabaseOpt.db.exesql(sql, row)

        # 添加下载任务
        self.request_head[id] = encode['MsgHead']
        self.request_head[id]['TaskID'] = encode['TaskID']
        self.dl.add_download_task([dl_url], DownloadDir, id, "CreateTemplate")

    def process_del_template_msg(self, encode):
        '''处理删除模板消息'''
        if encode["MsgHead"]["Cmd"] != "DeleteTemplate":
            self.loger.info("Msg {} can't processed process_del_template_msg!".format(encode))
            return -1
        id = encode["TaskID"]
        sql = "delete from  Template where TaskID='{}".format(id)
        row = ()
        ret = DatabaseOpt.db.exesql(sql, row)

        # 数据库的操作结果临时作为结果返回
        resultaddr = encode['MsgHead']['ResultURL']
        msg_id = encode['MsgHead']['MsgID']
        result_json = self.gen_del_template_result_msg(msg_id, ret)
        ResultSend.send(resultaddr, result_json)

    def process_match_image_msg(self, encode):
        '''处理匹配图片消息'''
        if encode["MsgHead"]["Cmd"] != "MatchImage":
            self.loger.info("Msg {} can't processed process_del_template_msg!".format(encode))
            return -1
        id = encode["MatchSessionID"]
        taskid = encode["TaskID"]
        templateid = encode["TemplateID"]
        image_url = encode["Image"]
        image_num = encode["ImageNum"]
        result_url = encode['MsgHead']['ResultURL']
        match_rec_image_path = os.path.join(DownloadDir,'match_image' + os.path.sep)
        if len(image_url) != image_num:
            pass
        sql = "insert into MatchSession(MatchSessionID,TaskID,TemplateID,Type,ObjPath,ResultURL,CreateTime) " \
              "values('{}','{}','{}','{}','{}','{}','{}')".format(id, taskid,  templateid, 1, match_rec_image_path,
                                                                  result_url,self.get_time_str())
        row = ()
        DatabaseOpt.db.exesql(sql, row)
        id = encode["MsgHead"]["MsgID"]
        self.request_head[id] = encode['MsgHead']
        self.request_head[id]['TaskID'] = encode['TaskID']
        img_ls = encode["Image"]
        url_ls = []
        img_type = []
        for img in img_ls:
            url_ls.append(img["ImageURL"])
            img_type.append(img["ImageType"]) #  图片类型，0：无票根 1：有票根
        self.dl.add_download_task(url_ls, match_rec_image_path, id, "MatchImage")

    def verify_msg(self, msg_json):
        msg_head = msg_json["MsgHead"]
        vh = self.verify_msg_head(msg_head)
        if vh == 0:
            cmd = msg_head['Cmd']
            if cmd == "CreateTemplate":
               is_exist = self.verify_template_isexist(msg_json["TemplateID"])
               if is_exist:
                   return 7
               else:
                   return 0
            elif cmd == "DeleteTemplate":
                is_exist = self.verify_template_isexist(msg_json["TaskID"])
                if is_exist:
                   return 0
                else:
                   return 7
            elif cmd == "MatchImage":
               pass
            elif cmd == "MatchVideo":
               pass
        else:
            return vh


    def verify_msg_head(self,msg_head):
        '''token=sha1(DateTime+Cmd+调用方口令)'''
        try:
            dt = msg_head["DateTime"]
            cmd = msg_head["Cmd"]
            passwd = 'easypai'
            list = [dt, cmd, passwd]
            list.sort()
            sha1 = hashlib.sha1()
            map(sha1.update, list)
            hashcode = sha1.hexdigest()
            self.loger.info("token:%s  local token:%s"%(msg_head["Token"],hashcode))
            if hashcode == msg_head["Token"]:
                return 0
            else :
                return 3
        except KeyError as e:
            self.loger.info("except:%s" % e)
            return 1

    def verify_template_isexist(self, templateid):
        sql = "select * from Template where TemplateID='%s'" % templateid
        row = []
        DatabaseOpt.db.exesql(sql, row)
        if len(row) == 0:
            return False
        else:
            return True

    def gen_resp(self, msgid, cmd, validata_rst):
        '''
           0：成功加入任务队列；1：语法错误；    2：没有权限；3：Token错误；
           4：任务不存在；      5：用户不存在；  6：内部错误；7:模板已存在
           8: 模板不存在
        '''
        tm = time.time()
        info = ""
        if validata_rst in self.response_desc.keys():
            info = self.response_desc[validata_rst]
        else:
            info = "other error"
        tm_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(tm))
        response={"Version": 1, "MsgID": msgid, "MsgType": "response", "DateTime": tm_str,
                  "ResponseName": cmd, "Return": validata_rst, "Desc": info}

        response_str = json.dumps(response)
        return response_str

    def gen_create_template_result_msg(self, id, result):
        '''{"Version":1,   "MsgID":12342,    "MsgType":"result",
            "DateTime":"2018-02-26 12:00:00" ,    "TaskID"： : "dd18110001"
            "MatchSessionID" : ""    "ResultName" : "CreateTemplate",
            "Result" : {“Status”："CreateDone"}
            “Desc”:”successful”}'''

        tm_str = self.get_time_str()
        request_head = self.request_head[id]
        status = "CreateDone" if result == 0 else "DownLoadTemplateFailed "
        result_json = {"Version": 1, "MsgID": id, "MsgType": "result",
            "DateTime": tm_str,    "TaskID": request_head["TaskID"],
            "MatchSessionID": "", 'ResultName': 'CreateTemplate',
            "Result": {'Status': status}, 'Desc': ''}
        return result_json

    def gen_del_template_result_msg(self, id, result):
        '''{"Version":1,   "MsgID":12342,    "MsgType":"result",
            "DateTime":"2018-02-26 12:00:00" ,    "TaskID"： : "dd18110001"
            "MatchSessionID" : ""    "ResultName" : "CreateTemplate",
            "Result" : {“Status”："CreateDone"}
            “Desc”:”successful”}'''

        rst_map = {0: "DeleteTemplateDone ", 1: "TemplateNoExist ", -1: "DeleteTemplateFailed "}
        tm_str = self.get_time_str()
        request_head = self.request_head[id]

        status = rst_map[result]
        result_json = {"Version": 1, "MsgID": id, "MsgType": "result",
            "DateTime": tm_str,    "TaskID": request_head["TaskID"],
            "MatchSessionID": "", 'ResultName': 'CreateTemplate',
            "Result": {'Status': status}, 'Desc': ''}
        return result_json

    def gen_match_image_err_result_msg(self, id, result):
        rst_map = {0: "DeleteTemplateDone ", 1: "TemplateNoExist ", -1: "DeleteTemplateFailed "}
        tm_str = self.get_time_str()
        request_head = self.request_head[id]

        status = rst_map[result]
        result_json = {"Version": 1, "MsgID": id, "MsgType": "result",
            "DateTime": tm_str,    "TaskID": request_head["TaskID"],
            "MatchSessionID": "", 'ResultName': 'CreateTemplate',
            "Result": {'Status': status}, 'Desc': ''}
        return result_json


    def get_time_str(self):
        tm = time.time()
        tm_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(tm))
        return tm_str

if __name__ == '__main__':
    msg = Msg()
    msg.start()
    info = {"MsgHead":{"Version":1, "Invoker": "user1", "ResultURL": "http://10.7.75.78:8080/result.php",
            "MsgID":12341, "MsgType":"request", "DateTime":"2018-02-26 12:00:00", "Cmd": "CreateTemplate",
            "Token":"da39a3ee5e6b4b0d3255bfef95601890afd80709"}, "TaskID":"DD18110001", "TemplateID":"15d8c0ab-caa9-11e8-9c79-0235d2b38928" ,
            "TemplateName":"可口可乐",
            "TemplateURL":"https://files.pythonhosted.org/package/78/d8/6e58a7130d457edadb753a0ea5708e411c100c7e94e72ad4802feeef735c/pip-1.5.4.tar.gz"
             }
    msg.add_task_queue(info)






