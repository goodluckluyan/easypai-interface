# -*- coding:utf-8 -*-
import web
import json
import Msg
import mylog
import traceback
# from web.wsgiserver import CherryPyWSGIServer
# ssl_cert = 'D:\\Project\\easypai\\bin\\myserver.crt'
# ssl_key = 'D:\\Project\\easypai\\bin\\myserver.key'
# CherryPyWSGIServer.ssl_certificate = ssl_cert
# CherryPyWSGIServer.ssl_private_key = ssl_key
urls = (
 '/easypai', 'Handle'
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
            web_data = web.data()
            mylog.logger.info("Handle Post webdata is %s" % web_data)  # 后台打日志
            web_data_u = web_data.decode('utf-8')
            encode = json.loads(web_data_u)
            mylog.logger.info(web_data_u)
            preprocess_stat = 0
            if "MsgHead" in encode.keys():
                head_validate = msg_processor.verify_msg(encode)
                if head_validate == 0:
                    msg_processor.add_task_queue(encode)
                preprocess_stat = head_validate
            else:
                preprocess_stat = 1
            cmdwd = encode["MsgHead"]["Cmd"]
            response_msg = msg_processor.gen_resp(encode["MsgHead"]["MsgID"], cmdwd, preprocess_stat)
            mylog.logger.info(response_msg)
            return response_msg


        except Exception as Argment:
            traceback()
            return Argment

if __name__ == '__main__':
    webapp = web.application(urls, globals())
    webapp.run()


