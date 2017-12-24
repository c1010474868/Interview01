import requests
from retrying import retry
from lxml import etree
from pymysql import *
import threading
from queue import Queue
import logging


class LuYing:

    def __init__(self):
        self.url = "http://www.jc.net.cn/market/search.html?keys=&area_name=&province=0&city=0&t1=%E9%BB%91%E8%89%B2%E5%8F%8A%E6%9C%89%E8%89%B2%E9%87%91%E5%B1%9E&t2=%E9%92%A2%E4%B8%9D%E7%BB%B3&st=&jgjs=&pno={}"
        self.headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36",
                   }

        self.url_queue = Queue()
        self.html_queue = Queue()
        self.content_list_queue = Queue()

    def get_url_list(self):
        url_list = [self.url.format(i) for i in range(1, 51)]
        for url in url_list:
            self.url_queue.put(url)

    @retry(stop_max_attempt_number=3)
    def _parse_url(self, url):
        r = requests.get(url, headers=self.headers, timeout=5)
        assert r.status_code == 200
        return etree.HTML(r.content)

    def parse_url(self):
        while True:
            url = self.url_queue.get()
            print(url)
            try:
                html = self._parse_url(url)
            except:
                html = None
            self.html_queue.put(html)
            self.url_queue.task_done()

    def get_content_list(self):
        while True:
            html = self.html_queue.get()
            if html is not None:
                tr_list = html.xpath("//tbody[@id='tbody']/tr")[1:-1]
                content_list = []
                for tr in tr_list:
                    item = dict()
                    item["name"] = tr.xpath("./td[1]/a/text()")[0]
                    item["brand"] = tr.xpath("./td[2]/text()")[0] if len(tr.xpath("./td[2]/text()")) > 0 else None
                    item["model"] = tr.xpath("./td[3]/text()")[4] if len(tr.xpath("./td[3]/text()")) > 0 else None
                    if item["model"]:
                        item["model"] = item["model"].strip().replace('\n', '').replace('\t', '')
                    item["unit"] = tr.xpath("./td[8]/text()")[0] if len(tr.xpath("./td[8]/text()")) > 0 else None
                    item["company"] = tr.xpath("./td[9]/a/text()")[0]
                    item["datetime"] = tr.xpath("./td[10]/text()")[0]
                    content_list.append(item)
                self.content_list_queue.put(content_list)
            self.html_queue.task_done()

    def connect_DB(self):
        con = connect(host='127.0.0.1', port=3306, database='test_l', user='root', password='mysql', charset='utf8')
        return con

    def create_DB(self):
        create_db = self.connect_DB()
        create_cursor = create_db.cursor()
        create_cursor.execute("drop table if exists luying1")
        print('-'*30)
        sql_luying = "create table luying1(id int auto_increment primary key,name varchar(40),brand varchar(40),model varchar(400),unit varchar(40),company varchar(40),datatime varchar(40))"
        create_cursor.execute(sql_luying)
        print('-'*30)
        create_cursor.close()

    def insert_DB(self):
        insert_db = self.connect_DB()
        insert_cursor = insert_db.cursor()

        while True:
            content_list = self.content_list_queue.get()
            print('^' * 25)
            for i in range(len(content_list)):
                name = content_list[i]["name"]
                brand = content_list[i]["brand"]
                model = content_list[i]["model"]
                unit = content_list[i]["unit"]
                company = content_list[i]["company"]
                datetime = content_list[i]["datetime"]
                sql_l_insert = ("insert into luying1(name, brand, model, unit, company, datatime) values(%s, %s, %s, %s, %s, %s)")
                sql_l_data = (name, brand, model, unit, company, datetime)
                insert_cursor.execute(sql_l_insert, sql_l_data)
                insert_db.commit()
            self.content_list_queue.task_done()

        insert_cursor.close()
        insert_db.close()

    def run(self):
        self.create_DB()
        thread_list = []
        t_url = threading.Thread(target=self.get_url_list)
        thread_list.append(t_url)
        for i in range(5):
            t_parse = threading.Thread(target=self.parse_url)
            thread_list.append(t_parse)
        for i in range(3):
            t_get_content_list = threading.Thread(target=self.get_content_list)
            thread_list.append(t_get_content_list)
        for i in range(3):
            t_save = threading.Thread(target=self.insert_DB)
            thread_list.append(t_save)

        for t in thread_list:
            t.setDaemon(True)
            t.start()

        for q in [self.url_queue, self.html_queue, self.content_list_queue]:
            q.join()

if __name__ == '__main__':
    luying = LuYing()
    luying.run()