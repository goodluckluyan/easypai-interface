# -*- coding:utf-8 -*-
import web
import json
import Msg
import mylog
import time
urls = (
 '/simulateuser','Handle'
)


msg_processor = Msg.Msg()
msg_processor.start()
class Handle(object):
    def GET(self):
        try:
            data = web.input()
            if len(data) == 0:
                return "hello, this is EasyPai"
        except Exception as Argument:
            return Argument

    def POST(self):
        try:
            webData = web.data()
            mylog.logger.info("Handle Post webdata is %s"%webData) #后台打日志
            encode = json.loads(webData.decode('utf-8'))
            mylog.logger.info(webData.decode('utf-8'))
            tm = time.time()
            tm_str = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(tm))
            response_json = {"Version":1,"MsgID":encode["MsgID"],"MsgType":"Result-Response",
                            "DateTime":tm_str,"Result":0,"Desc":""}
            response_msg = json.dumps(response_json)

            return response_msg

        except Exception as Argment:
            return Argment

if __name__ == '__main__':
    webapp = web.application(urls,globals())
    webapp.run()


