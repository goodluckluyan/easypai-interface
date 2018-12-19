# -*- coding:utf-8 -*-
from urllib import parse,request
import json
from queue import Queue
import threading
class MsgResultPost:
    # def __init__(self):
    #     self.post_queue = Queue()
    #
    # def start(self):
    #     self.h_thread = threading.Thread(target=self.send)
    #     self.h_thread.start()
    #
    # def add_send_msg(self,item):
    #     self.post_queue.put(item)

    def send(self, url, result_json):
        result_txt = json.dumps(result_json).encode(encoding='utf-8')
        print(result_txt)
        header_dict = {'User-Agent': 'Mozilla/5.0', "Content-Type": "application/json"}

        req = request.Request(url=url,data=result_txt,headers=header_dict)
        res = request.urlopen(req)
        res = res.read()
        print(res.decode(encoding='utf-8'))

ResultSend = MsgResultPost()