# -*- coding: utf-8 -*-
import re
import urllib2  
import sqlite3
import random
import threading
from bs4 import BeautifulSoup
import mysql.connector
import time

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

#设置代理池, 让反爬虫策略失效
proxys= [
None,
# {'http': '121.42.140.113:16816'},
# {'http': '101.234.76.118:16816'},
# {'http': '115.29.38.207:16816'},
# {'http': '101.203.173.104:16816'},
# {'http': '42.123.91.247:16816'},
# {'http': '118.123.22.209:16816'},
# {'http': '27.54.242.222:16816'},
{'http': '115.28.150.197:16816'}
]
openerIndex=0

done=0


#Some User Agents
hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Host': 'passport.lianjia.com',
    'Pragma': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36',
    'Set-Cookie': 'lianjia_ssid=116e556b-4558-4343-919f-cf883a4ee3d3; expires=Mon, 26-Dec-16 04:20:44 GMT; Max-Age=1800; domain=.lianjia.com; path=/'
}

i_headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.48'}
    

#北京区域列表
regions=[u"东城",u"西城",u"朝阳",u"海淀",u"丰台",u"石景山",u"通州",u"昌平",u"大兴",u"亦庄开发区",u"顺义",u"房山",u"门头沟",u"平谷",u"怀柔",u"密云",u"延庆",u"燕郊"]


lock = threading.Lock()

def setProxy(index):
    proxy_support = urllib2.ProxyHandler(proxys[index])
    opener = urllib2.build_opener(proxy_support)
    urllib2.install_opener(opener)

setProxy(0)
def gen_xiaoqu_insert_command(info_dict, conn):
    """
    生成小区数据库插入命令
    """
    info_list=[u'小区名称',u'大区域',u'小区域',u'成交链接', u'基本信息', u'编号']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)

    cursor = conn.cursor()
    cursor.execute('select * from xiaoqu where bianhao = (%s)', (info_dict[u'编号'],))
    values = cursor.fetchall()
    if len(values) == 0:
        command = "insert into xiaoqu (xiaoquname,regionb,regions,chengjiaolink,baseinfo,bianhao) values ('%s','%s','%s','%s','%s','%s')" % t
        print command
        cursor.execute(command)
        conn.commit()
    cursor.close()


def gen_chengjiao_insert_command(info_dict,conn):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'编号',u'链接',u'房子名称',u'签约时间',u'签约单价',u'签约总价',u'基本信息1',u'基本信息2',u'基本信息3',u'小区编号',u'regionb',u'regions']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    cursor = conn.cursor()
    # cursor.execute('select * from chengjiao where bianhao = (%s)', (info_dict[u'编号'],))
    # values = cursor.fetchall()
    # if len(values) == 0:
    command = "insert into chengjiao (bianhao,href,housename,sign_time,unit_price,total_price,info1,info2,info3,xiaoqubianhao,regionb,regions) values ('%s','%s','%s','%s',%s,%s,'%s','%s','%s','%s','%s','%s')" % t
    #print command
    cursor.execute(command)
    conn.commit()
    cursor.close()

def add_total_region_command(conn, sign_time, regionb, price):
    cursor = conn.cursor()
    cursor.execute('select total from region_report where sign_time = (%s) and region= (%s)', (sign_time,regionb))
    values = cursor.fetchall()
    if len(values) == 0:
        command = "insert into region_report values ('%s','%s',%s,%s)" % (sign_time,regionb,1,price)
    else:
        command = "update region_report set total=total+1, aggregateacount=aggregateacount+%s where sign_time = '%s' and region= '%s'" % (price, sign_time, regionb)
    # print command
    cursor.execute(command)
    conn.commit()
    cursor.close()

def xiaoqu_spider(db_xq,url_page=u"http://bj.lianjia.com/xiaoqu/pg1rs%E6%98%8C%E5%B9%B3/"):
    """
    爬取页面链接中的小区信息
    """
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text,'lxml')
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exit(-1)
    except Exception,e:
        print e
        exit(-1)
    
    xiaoqu_list=soup.findAll('div',{'class':'info'})
    for xq in xiaoqu_list:
        info_dict={}

        housetitle = xq.find("div", {"class": "title"})  # html
        print u"小区名称 %s" % housetitle.get_text().strip()
        info_dict.update({u'小区名称': housetitle.get_text().strip()})
        url = housetitle.a.get('href');
        bianhao = re.findall(r'(\w*[0-9]+)\w*', url)
        print u"编号 %s" % bianhao
        info_dict.update({u'编号': bianhao[0]})

        # info_dict.update({u'小区名称':xq.find('a').text})

        houseinfo = xq.find("div", {"class": "houseInfo"})  # html
        info_dict.update({u'成交链接': houseinfo.a.get('href')})

        # positionInfo = xq.find("div", {"class": "positionInfo"})  # html
        # info_dict.update({u'基本信息': positionInfo.get_text().strip()})

        print u"位置 %s" % xq.find('a',{'class':'district'}).text
        info_dict.update({u'大区域': xq.find('a', {'class': 'district'}).text})
        info_dict.update({u'小区域': xq.find('a', {'class': 'bizcircle'}).text})
        # info=re.match(r".+>(.+)</a>.+>(.+)</a>.+</span>(.+)<span>.+</span>(.+)",content)
        # if info:
        #     info=info.groups()
        #     info_dict.update({u'大区域':xq.find('a',{'class':'district'}).text})
        #     info_dict.update({u'小区域':xq.find('a',{'class':'bizcircle'}).text})
        #     info_dict.update({u'小区户型':''})
        #     info_dict.update({u'建造时间':''})
        gen_xiaoqu_insert_command(info_dict, db_xq)
        # print command
        # cursor = conn.cursor();
        # cursor.execute(command);
        # # db_xq.execute(command,1)

    
def do_xiaoqu_spider(db_xq,region=u"昌平"):
    """
    爬取大区域中的所有小区信息
    """
    url=u"http://bj.lianjia.com/xiaoqu/rs"+region+"/"
    print url
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=5).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text,'lxml')
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        return
    except Exception,e:
        print e
        return
    d="d="+soup.find('div',{'class':'page-box house-lst-page-box'}).get('page-data')
    exec(d)
    total_pages=d['totalPage']
    print total_pages
    
    threads=[]
    for i in range(total_pages):
        url_page=u"http://bj.lianjia.com/xiaoqu/pg%drs%s/" % (i+1,region)
        xiaoqu_spider(db_xq,url_page)
    #     t=threading.Thread(target=xiaoqu_spider,args=(db_xq,url_page))
    #     threads.append(t)
    # for t in threads:
    #     t.start()
    # for t in threads:
    #     t.join()
    print u"爬下了 %s 区全部的小区信息" % region


def chengjiao_spider(db_cj,url_page=u"http://bj.lianjia.com/chengjiao/pg1rs%E5%86%A0%E5%BA%AD%E5%9B%AD",xiaoqubianhao=u'1110138869312539', regionb=u'朝阳', regions=u'朝阳'):
    """
    爬取页面链接中的成交记录
    """
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')
        soup = BeautifulSoup(plain_text,'lxml')
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('chengjiao_spider',url_page)
        return
    except Exception,e:
        print e
        exception_write('chengjiao_spider',url_page)
        return

    authType = soup.find('p', {'id': 'authType'})
    if authType:
        print u'链家网流量异常, 停止抓取'
        return False

    content = soup.find('div', {'class': 'page-box house-lst-page-box'})
    total_pages = 0
    if content:
        d = "d=" + content.get('page-data')
        exec (d)
        total_pages = d['totalPage']

    if total_pages == 0:
        xiaoqu_chengjiao_done(db_cj, xiaoqubianhao)
        return True
    
    cj_list=soup.findAll('div',{'class':'info'})
    for cj in cj_list:
        info_dict={}
        title = cj.find("div", {"class": "title"})  # html
        if title == None:
            continue
        info_dict.update({u'房子名称': title.get_text().strip()})
        if title.a == None:
            continue
        url = title.a.get('href');
        if url == None:
            continue
        bianhao = re.findall(r'(\w*[0-9]+)\w*', url)

        info_dict.update({u'编号': bianhao[0]})
        info_dict.update({u'链接': url})
        info_dict.update({u'小区编号': xiaoqubianhao})

        dealDate = cj.find("div", {"class": "dealDate"})  # html
        if dealDate == None:
            continue
        info_dict.update({u'签约时间': dealDate.get_text().strip()})

        if dealDate.get_text().strip() < '2017.01.06':
            # print u"房子名称 %s %s" % (title.get_text().strip(), dealDate.get_text().strip())
            xiaoqu_chengjiao_done(db_cj, xiaoqubianhao)
            return True

        cursor = conn.cursor()
        cursor.execute('select * from chengjiao where bianhao = (%s)', (bianhao[0],))
        values = cursor.fetchall()
        if len(values) != 0:
            continue

        print u"房子名称 %s %s" % (title.get_text().strip(), dealDate.get_text().strip())

        totalPrice = cj.find("div", {"class": "totalPrice"})  # html
        if totalPrice == None:
            continue
        totalPrice1 = totalPrice.find("span", {"class": "number"})  # html
        if totalPrice1 == None:
            continue
        info_dict.update({u'签约总价': totalPrice1.get_text().strip()})

        unitPrice = cj.find("div", {"class": "unitPrice"})  # html
        if unitPrice == None:
            continue
        unitPrice1 = unitPrice.find("span", {"class": "number"})  # html
        if unitPrice1 == None:
            continue
        info_dict.update({u'签约单价': unitPrice1.get_text().strip()})

        houseinfo = cj.find("div", {"class": "houseInfo"})  # html
        if houseinfo == None:
            continue
        info_dict.update({u'基本信息1': houseinfo.get_text().strip()})

        positionInfo = cj.find("div", {"class": "positionInfo"})  # html
        if positionInfo == None:
            continue
        info_dict.update({u'基本信息2': positionInfo.get_text().strip()})

        dealHouseInfo = cj.find("div", {"class": "dealHouseInfo"})  # html
        if dealHouseInfo == None:
            continue
        info_dict.update({u'基本信息3': dealHouseInfo.get_text().strip()})


        info_dict.update({u'regionb': regionb})
        info_dict.update({u'regions': regions})

        print u"房子名称 %s" % title.get_text().strip()
        print u"编号 %s" % bianhao
        print u"房源URL %s" % url
        gen_chengjiao_insert_command(info_dict, db_cj)

    xiaoqu_chengjiao_done(db_cj, xiaoqubianhao)
    return True

def xiaoqu_chengjiao_done(db_cj, bianhao):
    cursor = db_cj.cursor()
    cursor.execute("update xiaoqu set baseinfo = '0' where bianhao = (%s)", (bianhao,))
    db_cj.commit()
    cursor.close()


def xiaoqu_chengjiao_spider(db_cj,xq_name=u"冠庭园", bianhao=u'1111027374674', regionb=u'朝阳', regions=u'朝阳'):
    """
    爬取小区成交记录
    """
    url=u"http://bj.lianjia.com/chengjiao/c"+urllib2.quote(bianhao)+"/"
    #print url
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text,'lxml')
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    except Exception,e:
        print e
        exception_write('xiaoqu_chengjiao_spider',xq_name)
        return
    content=soup.find('div',{'class':'page-box house-lst-page-box'})
    total_pages=0
    if content:
        d="d="+content.get('page-data')
        exec(d)
        total_pages=d['totalPage']

    print 'spidering %s 小区 %d' % (xq_name, total_pages)

    for i in range(total_pages):
        url_page=u"http://bj.lianjia.com/chengjiao/pg%dc%s/" % (i+1,urllib2.quote(bianhao))
        time.sleep(2)
        result = chengjiao_spider(db_cj,url_page,bianhao,regionb,regions)
        if result:
            continue
        else:
            break
    #     t=threading.Thread(target=chengjiao_spider,args=(db_cj,url_page))
    #     threads.append(t)
    # for t in threads:
    #     t.start()
    # for t in threads:
    #     t.join()

def beijing_chengjiao_spider(db_cj):
    """
    爬取最新成交记录
    """
    total_pages = 100
    print total_pages
    for i in range(total_pages):
        if i < done:
            continue
        url_page=u"http://bj.lianjia.com/chengjiao/pg%d/" % (i+1)
        time.sleep(3)
        result = zengliang_chengjiao_spider(db_cj,url_page)
        if result:
            continue
        else:
            break


def zengliang_chengjiao_spider(db_cj, url_page=u"http://bj.lianjia.com/chengjiao/pg1/"):
    """
    爬取页面链接中的成交记录
    """
    try:
        req = urllib2.Request(url_page, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib2.urlopen(req, timeout=20).read()
        plain_text = unicode(source_code)  # ,errors='ignore')
        soup = BeautifulSoup(plain_text, 'lxml')
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('chengjiao_spider', url_page)
        return
    except Exception, e:
        print e
        exception_write('chengjiao_spider', url_page)
        return

    cj_list = soup.findAll('div', {'class': 'info'})
    count = 0
    newCount = 0
    for cj in cj_list:
        count +=1
        info_dict = {}
        title = cj.find("div", {"class": "title"})  # html
        if title == None:
            continue
        houseTitle = title.get_text().strip()
        info_dict.update({u'房子名称': title.get_text().strip()})
        if title.a == None:
            continue
        url = title.a.get('href');
        if url == None:
            continue
        bianhao = re.findall(r'(\w*[0-9]+)\w*', url)

        info_dict.update({u'编号': bianhao[0]})
        info_dict.update({u'链接': url})

        cursor = conn.cursor()
        xiaoquName = title.get_text().strip().split(" ", 1)[0];
        regionb = ''
        #print  xiaoquName;
        cursor.execute("select bianhao,regionb,regions from xiaoqu where xiaoquname = (%s)", (xiaoquName,))
        values = cursor.fetchall()
        if len(values) != 0:
            xiaoqubianhao = values[0][0];
            info_dict.update({u'小区编号': xiaoqubianhao})
            info_dict.update({u'regionb': values[0][1]})
            regionb = values[0][1]
            info_dict.update({u'regions': values[0][2]})
            # print xiaoqubianhao;
        else:
            print u"%s 小区不存在" % houseTitle;
            continue

        cursor.execute('select * from chengjiao where bianhao = (%s)', (bianhao[0],))
        values = cursor.fetchall()
        if len(values) != 0:
            #print u"成交房源编号存在 %s" % bianhao
            continue

        # print u"成交房源编号不存在 %s" % bianhao
        newCount +=1

        totalPrice = cj.find("div", {"class": "totalPrice"})  # html
        if totalPrice == None:
            continue
        totalPrice1 = totalPrice.find("span", {"class": "number"})  # html
        if totalPrice1 == None:
            continue
        info_dict.update({u'签约总价': totalPrice1.get_text().strip()})


        unitPrice = cj.find("div", {"class": "unitPrice"})  # html
        if unitPrice == None:
            continue
        unitPrice1 = unitPrice.find("span", {"class": "number"})  # html
        if unitPrice1 == None:
            continue
        info_dict.update({u'签约单价': unitPrice1.get_text().strip()})
        unitPriceNum = unitPrice1.get_text().strip()

        houseinfo = cj.find("div", {"class": "houseInfo"})  # html
        if houseinfo == None:
            continue
        info_dict.update({u'基本信息1': houseinfo.get_text().strip()})

        positionInfo = cj.find("div", {"class": "positionInfo"})  # html
        if positionInfo == None:
            continue
        info_dict.update({u'基本信息2': positionInfo.get_text().strip()})

        dealHouseInfo = cj.find("div", {"class": "dealHouseInfo"})  # html
        if dealHouseInfo == None:
            continue
        info_dict.update({u'基本信息3': dealHouseInfo.get_text().strip()})

        dealDate = cj.find("div", {"class": "dealDate"})  # html
        if dealDate == None:
            continue
        info_dict.update({u'签约时间': dealDate.get_text().strip()})
        sign_time = dealDate.get_text().strip()

        # print u"房子名称 %s" % title.get_text().strip()
        # print u"编号 %s" % bianhao
        # print u"房源URL %s" % url
        gen_chengjiao_insert_command(info_dict, db_cj)
        add_total_region_command(conn, sign_time, regionb, unitPriceNum)

    print u"%s 处理了 %d 套成交房源, 新房源: %d套" % (url_page,count,newCount)
    return True
    
def do_xiaoqu_chengjiao_spider(db_xq,db_cj):
    """
    批量爬取小区成交记录
    """
    count=0
    cursor = db_xq.cursor()
    cursor.execute('select * from xiaoqu ')
    xq_list = cursor.fetchall()
    for xq in xq_list:
        xiaoqu_chengjiao_spider(db_cj,xq[1],xq[0],xq[2],xq[3])
        time.sleep(3)
        count+=1
    print 'have spidered %d xiaoqu' % count
    print 'done'

def do_xiaoqu_chengjiao_again_spider(db_xq,db_cj):
    """
    批量爬取小区成交记录
    """
    count=0
    cursor = db_xq.cursor()
    cursor.execute('select * from xiaoqu where baseinfo ="" ORDER BY bianhao')
    xq_list = cursor.fetchall()
    for xq in xq_list:
        time.sleep(3)
        url = u"http://bj.lianjia.com/chengjiao/c" + urllib2.quote(xq[0]) + "/"
        # xiaoqu_chengjiao_spider(db_cj,xq[1],xq[0],xq[2],xq[3])
        result = chengjiao_spider(db_cj, url, xq[0],xq[2],xq[3])
        if result:
            count+=1
            index = count % len(proxys)
            setProxy(index)
            print 'have spidered %d xiaoqu, the latest is: %s %s %s %d' % (count,xq[0],xq[1],xq[4],index)
        else:
            return
    print 'done'

def do_batch_detail_chengjiao_spider(db_xq):
    """
    批量爬取小区成交记录
    """
    count=0
    cursor = db_xq.cursor()
    cursor.execute("SELECT a.bianhao, href, a.housename FROM chengjiao a WHERE a.xiaoqubianhao = ''")
    xq_list = cursor.fetchall()
    for xq in xq_list:
        print 'spidering %s ' % xq[2]
        do_detail_spider(db_xq,xq[0],xq[2])
        count+=1
    print 'have spidered %d xiaoqu' % count
    print 'done'

def do_detail_spider(db_xq, bianhao, housename):


    cursor = conn.cursor()
    xiaoquName = housename.split(" ", 1)[0];
    # print  xiaoquName;
    cursor.execute("select bianhao from xiaoqu where xiaoquname = (%s)", (xiaoquName,))
    values = cursor.fetchall()
    if len(values) != 0:
        xiaoqubianhao = values[0][0];
        print xiaoqubianhao;
    else:
        print url_page;
        print u"%s 小区不存在" % xiaoquName;
        return

    cursor.execute("update chengjiao set xiaoqubianhao=(%s) where  bianhao = (%s)", (xiaoqubianhao,bianhao))
    conn.commit()
    cursor.close()


def exception_write(fun_name,url):
    """
    写入异常信息到日志
    """
    lock.acquire()
    f = open('log.txt','a')
    line="%s %s\n" % (fun_name,url)
    f.write(line)
    f.close()
    lock.release()


def exception_read():
    """
    从日志中读取异常信息
    """
    lock.acquire()
    f=open('log.txt','r')
    lines=f.readlines()
    f.close()
    f=open('log.txt','w')
    f.truncate()
    f.close()
    lock.release()
    return lines


def exception_spider(db_cj):
    """
    重新爬取爬取异常的链接
    """
    count=0
    excep_list=exception_read()
    while excep_list:
        for excep in excep_list:
            excep=excep.strip()
            if excep=="":
                continue
            excep_name,url=excep.split(" ",1)
            if excep_name=="chengjiao_spider":
                chengjiao_spider(db_cj,url)
                count+=1
            elif excep_name=="xiaoqu_chengjiao_spider":
                xiaoqu_chengjiao_spider(db_cj,url)
                count+=1
            else:
                print "wrong format"
            print "have spidered %d exception url" % count
        excep_list=exception_read()
    print 'all done ^_^'
    
#=========================setup a database, only execute in 1st running=================================
def database_init(dbflag='local'):
     if dbflag=='local':
         conn = mysql.connector.connect(user='root', password='root', database='jeesite',host='localhost')
     else:
         conn = mysql.connector.connect(user='qdm', password='password', database='qdm',host='qdm.my3w.com')
     dbc = conn.cursor()

     # 创建houseinfo and hisprice表:
     dbc.execute('create table if not exists houseinfo (id int(10) NOT NULL AUTO_INCREMENT primary key,houseID varchar(50) , Title varchar(200), link varchar(200), cellname varchar(100),\
                years varchar(200),housetype varchar(50),square varchar(50), direction varchar(50),floor varchar(50),taxtype varchar(200), \
                totalPrice varchar(200), unitPrice varchar(200),followInfo varchar(200),validdate varchar(50),validflag varchar(20))')

     dbc.execute('create table if not exists hisprice (id int(10) NOT NULL AUTO_INCREMENT primary key,houseID varchar(50) , date varchar(50), totalPrice varchar(200))')
     dbc.execute('create table if not exists cellinfo (id int(10) NOT NULL AUTO_INCREMENT primary key,Title varchar(50) , link varchar(200),district varchar(50),bizcircle varchar(50),tagList varchar(200))')

     dbc.execute("create table if not exists xiaoqu (name varchar(200) primary key UNIQUE, regionb varchar(200), regions varchar(200), style varchar(200), year varchar(200))")

     dbc.execute("create table if not exists chengjiao (href varchar(200) primary key UNIQUE, name varchar(200), style varchar(200), area varchar(200), orientation varchar(200), floor varchar(200), year varchar(200), sign_time varchar(200), unit_price varchar(200), total_price varchar(200),fangchan_class varchar(200), school varchar(200), subway varchar(200))")

     conn.commit()
     dbc.close()
     return conn

if __name__=="__main__":

    dbflag = 'local'  # local,  remote
    conn = database_init(dbflag)

    #第一步 爬下所有的小区信息
    #  for region in regions:
    #      do_xiaoqu_spider(conn,region)
    #
    # #第二步 爬下所有小区里的成交信息
    # do_xiaoqu_chengjiao_spider(conn,conn)
    #
    #
    # #有了第一步和第二部的基础数据, 日常只需增量爬最新100页成交
    beijing_chengjiao_spider(conn)

    # do_batch_detail_chengjiao_spider(conn);

    #长期没抓取, 补缺抓取
    # do_xiaoqu_chengjiao_again_spider(conn, conn)

    # zengliang_chengjiao_spider(conn, u'http://bj.lianjia.com/chengjiao/pg14/');
    # zengliang_chengjiao_spider(conn, u'http://bj.lianjia.com/chengjiao/pg32/');
    # zengliang_chengjiao_spider(conn, u'http://bj.lianjia.com/chengjiao/pg38/');
    # zengliang_chengjiao_spider(conn, u'http://bj.lianjia.com/chengjiao/pg39/');
    # zengliang_chengjiao_spider(conn, u'http://bj.lianjia.com/chengjiao/pg45/');
    
    #重新爬取爬取异常的链接
    # exception_spider(conn)

