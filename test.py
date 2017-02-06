import urllib2
from bs4 import BeautifulSoup
import json
import requests

proxyList = []
proxyIndex=0

def get_json_data(url):
    try:
        data = urllib2.urlopen(url).read()
        return data
    except Exception, e:
        print e

def get_proxyList():
    global proxyList
    global proxyIndex
    url = 'http://dev.kuaidaili.com/api/getproxy/?orderid=928618036841666&num=20&b_pcchrome=1&b_pcie=1&b_pcff=1&protocol=1&method=2&an_an=1&an_ha=1&sp1=1&sp2=1&dedup=1&format=json&sep=1'
    jsonData = get_json_data(url)
    value = json.loads(jsonData)
    session = requests.session()
    for proxyip in value['data']['proxy_list']:
        try:
            testurl = 'http://ip.chinaz.com/getip.aspx'
            response = session.get(testurl, proxies=proxyip, timeout=5)
            proxyList.append(proxyip)
            if (len(proxyList) == 10):
                break
        except Exception, e:
            continue
    proxyIndex=0

get_proxyList()
print proxyList

def set_next_proxy():
    global proxyList
    global proxyIndex
    proxyIndex += 1
    lenth = len(proxyList)
    if proxyIndex==lenth:
        get_proxyList()
    proxy = {'http':proxyList[proxyIndex]}
    proxy_support = urllib2.ProxyHandler(proxy)
    opener = urllib2.build_opener(proxy_support)
    urllib2.install_opener(opener)

