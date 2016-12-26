import urllib2
from bs4 import BeautifulSoup

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Host': 'bj.lianjia.com',
    'Pragma': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36',
    'Cookie': 'lianjia_uuid=c75d9791-741c-45b4-8f62-62e54d178134; miyue_hide=%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20%20index%20; _jzqa=1.4429873958313777700.1470807422.1473038223.1474190763.8; _jzqy=1.1471315506.1480493381.3.jzqsr=baidu.jzqsr=baidu; Hm_lvt_efa595b768cc9dc7d7f9823368e795f1=1479721012,1480475204; select_city=110000; _jzqx=1.1473049033.1482718746.10.jzqsr=bj%2Elianjia%2Ecom|jzqct=/fangjia/.jzqsr=captcha%2Elianjia%2Ecom|jzqct=/; _jzqckmp=1; all-lj=59dc31ee6d382c2bb143f566d268070e; _smt_uid=57aabd7e.2f9380d7; CNZZDATA1253477573=1339562570-1470803889-%7C1482734313; CNZZDATA1254525948=1191639991-1470803082-%7C1482734117; CNZZDATA1255633284=1693320555-1470803287-%7C1482730077; CNZZDATA1255604082=706716758-1470803344-%7C1482732682; _qzja=1.1942059574.1470807422435.1482734023733.1482734370648.1482734370648.1482734385126.0.0.0.678.95; _qzjc=1; _qzjto=45.6.0; _jzqa=1.4429873958313777700.1470807422.1473038223.1474190763.8; _jzqc=1; _ga=GA1.2.554288643.1470205161'
}

def url_user_agent(url):
    proxy = {'http':'110.73.3.152:8123'}
    proxy_support = urllib2.ProxyHandler(proxy)
    # opener = urllib2.build_opener(proxy_support,urllib2.HTTPHandler(debuglevel=1))
    opener = urllib2.build_opener(proxy_support)
    opener.add_handler = [('User-Agent','Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36')]
    urllib2.install_opener(opener)

    # i_headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
    i_headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
    req = urllib2.Request(url,headers=i_headers)
    html = urllib2.urlopen(req)
    plain_text = unicode(html)
    soup = BeautifulSoup(plain_text, 'lxml')
    print soup
    return

url = 'http://bj.lianjia.com/chengjiao'
doc = url_user_agent(url)
print doc