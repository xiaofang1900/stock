import tushare as ts
import pandas as pd
import numpy as np
import datetime
import sqlite3 as lite
import time
import traceback
import urllib2
import requests
import threading
import sys,os
def main(args):
    data = pd.read_csv("~/tmp/stock/stock_basics.csv")
    skip_code = []
    if os.path.isfile("/home/fangdegao/tmp/done.txt"):
        with open("/home/fangdegao/tmp/done.txt") as fh:
            line = fh.readline()
            if line is not None:
                skip_code = line.split()

    # #data.to_csv("~/tmp/stock/stock_basics.csv")
    allcodes = data['code']

    # print allcodes.values
    g_end_day=datetime.date(datetime.date.today().year,datetime.date.today().month,datetime.date.today().day)
    mProxy = ProxyProvider()
    for code in allcodes:
            code = '%06d' % code
            if code in skip_code:
                continue
            #code = '300397'
            print "begin code :" + code
            end_day = g_end_day
            outfile = '/home/fangdegao/tmp/stock/week/%s.csv' % (code)
            codeinfo = None
            if os.path.exists(outfile):
                codeinfo = pd.read_csv(outfile,index_col=0,parse_dates=True,infer_datetime_format=True)
                #codeinfo.set_index("date",inplace=True)
            if codeinfo is not None:
                try:
                    end_day = codeinfo.index[-1]
                except Exception, e:
                    traceback.print_exc()

            ealybreak = False
            for i in range(1,20):
                days = i * 200
                start_day = end_day - datetime.timedelta(days)
                tmp_end_day = start_day + datetime.timedelta(199)
                str_start = start_day.strftime("%Y-%m-%d")
                str_end = tmp_end_day.strftime("%Y-%m-%d")
                print str_start,str_end
                while( True) :
                    stockInfo = getStockInfo(code,str_start,str_end)
                    if( stockInfo[0] == 1):
                        if( stockInfo[1].values.size == 0):
                            ealybreak = True
                        if codeinfo is None:
                            codeinfo = stockInfo[1]
                        else :
                            #print codeinfo.tail()
                            codeinfo = codeinfo.append(stockInfo[1])
                            #print codeinfo.tail()
                        break
                    else :
                        try:
                            proxy = mProxy.pick()
                            print proxy
                            installProxy(proxy[0],proxy[1])
                        except Exception,e:
                            traceback.print_exc()
                if ealybreak :
                    print "end " + code
                    skip_code.append(code)
                    break;




            if not (codeinfo is None):
                #print codeinfo.tail()
                #codeinfo.sort_index()
                print "save file: " + outfile
                codeinfo.to_csv(outfile)
            donefile = open("/home/fangdegao/tmp/done.txt", 'w')
            donefile.write(" ".join(skip_code))
            donefile.close()


def getStockInfo(code,start,end):
    try:
        data = ts.get_hist_data(code,start, end,ktype='W')
        #print data.head()
        #print data.tail()
        return [1,data]
    except  Exception, e:
        return [0,None]


def installProxy(ip , port):
    proxydict = {}
    proxydict['http'] = "http://%s:%s" % (ip, port)
    # print proxydict
    proxy_handler = urllib2.ProxyHandler(proxydict)
    opener = urllib2.build_opener(proxy_handler)
    urllib2.install_opener(opener)

class ProxyProvider:
    def __init__(self, min_proxies=200):
        self._bad_proxies = {}
        self._minProxies = min_proxies
        self.lock = threading.RLock()
        self.get_list()
        self.curIndex = 0

    def get_list(self):
        r = requests.get("http://127.0.0.1:8000/", timeout=10)
        ip_ports = pd.json.loads(r.text)
        self._proxies = ip_ports

    def pick(self):
        with self.lock:
            proxy = self._proxies[self.curIndex]
            self.curIndex = (self.curIndex + 1) % len(self._proxies)
            return proxy

if __name__ == "__main__":
    main(sys.argv[1:])