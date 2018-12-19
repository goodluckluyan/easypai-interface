#!/usr/bin/python
#encoding:utf-8
from urllib import request
from urllib import error as  urlerr
from queue import Queue
import threading
import os
import sys
import socket
import mylog

class FileDownload:
    def __init__(self,download_msg_queue=None,logger=None):
        self.dl_queue = Queue()
        self.is_run = True
        self.dl_msg_queue = download_msg_queue
        self.logger = mylog.logger
        self.h_thread = threading.Thread(target=self.download_process)

    def exe_download(self, url, savepath):
        try:
            request.urlretrieve(url, savepath)
        except socket.timeout:
             count = 1
             while count <= 5:
                try:
                    request.urlretrieve(url, savepath)
                    break
                except socket.timeout:
                    err_info = 'Reloading for %d time' % count if count == 1 else 'Reloading for %d times' % count
                    # print(err_info)
                    self.logger.info(err_info)
                    count += 1
             if count > 5:
                # print("downloading picture fialed!")
                self.logger.info("downloading file fialed,tyr more than 5 times!")
                return 1
        return 0

    def download_process(self):
        while self.is_run:
            node = self.dl_queue.get()
            url_ls = node[0]
            savepath = node[1]
            # print("start download %s"%url)
            dl_ret_ls = []
            for url_item in url_ls:
                try:

                    self.logger.info("start download %s" % url_item)
                    filename = url_item.split('/')[-1]
                    if savepath[-1] != os.path.sep:
                        savepath += os.path.sep
                    savepath = os.path.join(savepath, filename)
                    dl_ret = self.exe_download(url_item, savepath)
                    dl_ret_ls.append(dl_ret)
                except urlerr.HTTPError as e:
                    dl_ret = 1
                    # print("%s download failed(%s)"%(savepath,e))
                    self.logger.info("%s download failed(%s)" % (savepath, e))
                    dl_ret_ls.append(dl_ret)

            if sum(dl_ret_ls) == 0:
                # print("%s download complete!\n"%savepath)
                self.logger.info("%s download complete!" % savepath)
            else:
                # print("%s download failed\n"%(savepath))
                self.logger.info("%s download failed" % (savepath))

            self.dl_queue.task_done()
            if self.dl_msg_queue != None:
                id = node[2]
                type = node[3]
                self.dl_msg_queue.put([id, type, dl_ret_ls])

    def add_download_task(self, url_ls, savepath, id=0, type=""):
        node = [url_ls, savepath, id, type]
        self.dl_queue.put(node)

    def start(self):
        self.h_thread.start()

def Schedule(a,b,c):
        '''
        a:已经下载的数据块
        b:数据块的大小
        c:远程文件的大小
       '''
        per = 100.0 * a * b / c
        print("%.2f%%"%(per))
        if per > 100 :
            per = 100
            print('%s %.2f%%'%(per))

if __name__ == '__main__':
    url = 'https://files.pythonhosted.org/package/78/d8/6e58a7130d457edadb753a0ea5708e411c100c7e94e72ad4802feeef735c/pip-1.5.4.tar.gz'
    fd = FileDownload()
    fd.start()
    fd.add_download_task([url],'d:\\')