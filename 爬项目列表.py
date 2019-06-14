import os
from bs4 import BeautifulSoup
import threading, json, time, requests
from queue import Queue
import math

cur_path = os.path.dirname(__file__)


#列表首页
baseurl='https://cy.ncss.cn/search/projects'
#数据总数
datacounturl='https://cy.ncss.cn/search/projectcount?name=&industryCode=&wasBindUniTechnology=&investStageCode=&provinceCode=&pageIndex=0&pageSize=15'

pageSize=15 #固定每页15挑
totalCount=0 # 总数，需要先去get到
pageCount=0 #总页数  math.ceil(totalCount/pageSize)


class ThreadCrawl(threading.Thread):
    def __init__(self, thread_name, pageQueue, dataQueue):
        super(ThreadCrawl, self).__init__()  # 调用父类初始化方法
        self.thread_name = thread_name  # 线程名
        self.pageQueue = pageQueue  # 页码队列
        self.dataQueue = dataQueue  # 数据队列
        self.headers = {"User-Agent": "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;"}

    def run(self):

        while not self.pageQueue.empty():  # 如果pageQueue为空，采集线程退出循环 Queue.empty() 判断队列是否为空
            try:
                # 取出一个数字，先进先出
                # 可选参数block，默认值为True
                # 1. 如果对列为空，block为True的话，不会结束，会进入阻塞状态，直到队列有新的数据
                # 2. 如果队列为空，block为False的话，就弹出一个Queue.empty()异常，
                page = self.pageQueue.get(False)
                url = 'https://cy.ncss.cn/search/projectlist?name=&industryCode=&wasBindUniTechnology=&investStageCode=&provinceCode=&pageIndex='+str(page)+'&pageSize=15&_=1559172869435'#"http://www.qiushibaike.com/8hr/page/" + str(page) + "/"
                print(url)
                html = requests.get(url, headers=self.headers).text
                time.sleep(1)  # 等待1s等他全部下完


                self.dataQueue.put((page,html))

            except Exception as e:
                print('ThreadCrawl error catch')
                print(e)
                pass

        #print("结束" + self.thread_name)

def textfilter(text):
    if text ==None:
        return ''

    '''去除多余字符'''

    text=text.replace('space','')
    text=text.replace('\n','')
    text=text.replace('\r','')

    print(text)
    return text

import redis
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)   # host是redis主机，需要redis服务端和客户端都起着 redis默认端口是6379
r = redis.Redis(connection_pool=pool)
pointkey='cyncss_projects'

class ThreadParse(threading.Thread):
    def __init__(self, threadName, dataQueue, lock):
        super(ThreadParse, self).__init__()
        self.threadName = threadName
        self.dataQueue = dataQueue
        self.lock = lock  # 文件读写锁
        self.headers = {"User-Agent": "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;"}

    def run(self):
        while not self.dataQueue.empty():  # 如果pageQueue为空，采集线程退出循环
            try:

                page,html = self.dataQueue.get()  # 解析为HTML DOM
                soup = BeautifulSoup(html, "lxml");
                table = soup.table;
                tr_arr = table.find_all("tr");
                if tr_arr==None:
                    return
                for tr in tr_arr:
                    div_title = tr.find("div", attrs={"class": "project-title"});
                    if div_title == None:

                        continue
                    titlechar = textfilter(div_title.text)


                    div_synopsis = tr.find("div", attrs={"class": "synopsis"});
                    synopsischar = textfilter(div_synopsis.text)

                    tds = tr.find_all("td", attrs={"class": "text-center"});
                    type=textfilter(tds[0].text)
                    addr=textfilter(tds[1].text)
                    ifcompany=textfilter(tds[2].text)
                    step=textfilter(tds[3].text)
                    items = {
                        "title": titlechar,#项目标题
                        'synopsis':synopsischar,#项目描述
                        'type':type,#融资类型
                        'addr': addr,# 所在地
                        'ifcompany': ifcompany,# 是否注册公司
                        'step': step,#融资阶段
                    }

                    #写入缓存
                    r.hset(pointkey+str(page),titlechar.strip().encode("utf-8",'ignore'),json.dumps(items, ensure_ascii=False).encode("utf-8",'ignore'))

            except Exception as e:
                print('error====================================')
              
                print(e)
                print('error====================================')
                

flag1=[0]

pageQueue = Queue()  # 页码的队列，表示10个页面，不写表示不限制个数

def deprocess(start):
    end=0
    fla=0
    for i in range(start, pageCount + 1):  # 放入1~10的数字，先进先出
        # 队列太多  分批加入队列，10条队列处理一批
        pageQueue.put(i)
        fla+=1
        if fla==10:
            end=i
            break
        flag1.append(i)
    # 分配好队列，开启多线程处理队列 ，但是不是立即return ,多线程任务相对于这里，是阻塞的
    return processPageData(end)

def main():
    req = requests.get(datacounturl)

    result = req.text
    if result == '':
        return

    result = json.loads(result)
    if result != '':
        totalCount = int(result)
        global pageCount
        pageCount = math.ceil(totalCount / pageSize)

    print('总数据=%s 每页%s条总共 %s页数据' % (totalCount, pageSize, pageCount))

    try:
        global flag1
        while len(flag1) == 1:

            flag1 = deprocess(flag1[0])
            print(flag1)
    except Exception as e:
        print('main')
        print(e)



    print("谢谢使用！")


def processPageData(flag):
    print('结束位置 %s'% str(flag))

    dataQueue = Queue()  # 采集结果(每页的HTML源码)的数据队列，参数为空表示不限制个数
    crawlList = ["采集线程1号", "采集线程2号", "采集线程3号", "采集线程4号", "采集线程5号", "采集线程6号"]  # 存储三个采集线程的列表集合，留着后面join（等待所有子进程完成在退出程序）

    threadcrawl = []
    for thread_name in crawlList:
        thread = ThreadCrawl(thread_name, pageQueue, dataQueue)
        thread.start()
        threadcrawl.append(thread)

    for i in threadcrawl:
        i.join()


    lock = threading.Lock()  # 创建锁

    # *** 解析线程一定要在采集线程join（结束）以后写，否则会出现dataQueue.empty()=True（数据队列为空），因为采集线程还没往里面存东西呢 ***
    parseList = ["解析线程1号", "解析线程2号", "解析线程3号", "解析线程4号", "解析线程5号", "解析线程6号"]  # 三个解析线程的名字
    threadparse = []  # 存储三个解析线程，留着后面join（等待所有子进程完成在退出程序）
    for threadName in parseList:
        thread = ThreadParse(threadName, dataQueue, lock)
        thread.start()
        threadparse.append(thread)

    for j in threadparse:
        j.join()


    return [flag+1]
if __name__ == "__main__":
    main()