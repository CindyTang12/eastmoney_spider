import selenium
from selenium import webdriver
from selenium.webdriver.support.select import Select  # 导入Select包
from selenium.webdriver.common.action_chains import ActionChains  # 导入鼠标事件包
from time import sleep
import time
import pymysql
from selenium.webdriver.common.keys import Keys


class Spider:

    def __init__(self):
        self.wd = webdriver.Chrome()
        self.date = time.strftime("%Y-%m-%d", time.localtime())
        self.url = "http://data.eastmoney.com/futures/sh/data.html?date=" + self.date + "&ex=069001005&va=RB&ct=rb2010"
        sleep(2)
        self.wd.get(self.url)
        self.wd.implicitly_wait(10)
        self.varietyls = [{'s1_text': '上海期货交易所', 's1_value': '069001005', 's2_text': '螺纹钢', 's2_value': 'RB'},
                          {'s1_text': '大连商品期货交易所', 's1_value': '069001007', 's2_text': '铁矿石', 's2_value': 'I'},
                          {'s1_text': '郑州商品交易所', 's1_value': '069001008', 's2_text': '郑煤', 's2_value': 'ZC'}]
        self.numdict = {'永安期货': '80102901',
                        '中信期货': '80050220',
                        '银河期货': '80103797',
                        '一德期货': '80102904',
                        '方正中期期货': '80066668'}
        self.future_link_dict = {'螺纹钢': 'http://quote.eastmoney.com/center/gridlist2.html#futures_113_7',
                                 '铁矿石': 'http://quote.eastmoney.com/center/gridlist2.html#futures_114_13',
                                 '动力煤': 'http://quote.eastmoney.com/center/gridlist2.html#futures_115_17'}
        self.futures_contract = {}
        cow_close = self.wd.find_element_by_css_selector('#intellcontclose')
        ActionChains(self.wd).move_to_element(cow_close).click().perform()

    def getFutureHTML(self, infodict):
        s0 = self.wd.find_element_by_css_selector('#inputDate')
        sleep(2)
        if s0.get_attribute('value') != self.date:
            print('无法获取数据，还未到时间')
            exit()
            # ActionChains(self.wd).move_to_element(s0).click().perform()
            # sleep(1)
            # iframe = self.wd.find_elements_by_tag_name("iframe")[3]
            # self.wd.switch_to.frame(iframe)
            # today = self.wd.find_element_by_css_selector('.Wtoday')
            # ActionChains(self.wd).move_to_element(today).click().perform()
            # sleep(1)
            # self.wd.switch_to.default_content()
        s1 = self.wd.find_element_by_id("futures_exchange")  # 这里先找到select的标签的id
        Select(s1).select_by_visible_text(infodict['s1_text'])  # 通过文本值定位
        Select(s1).select_by_value(infodict['s1_value'])  # 通过value值定位
        sleep(2)
        # 选择该交易所需要的品种
        s2 = self.wd.find_element_by_id("futures_variety")  # 这里先找到select的标签的id
        Select(s2).select_by_visible_text(infodict['s2_text'])  # 通过文本值定位
        Select(s2).select_by_value(infodict['s2_value'])  # 通过value值定位
        sleep(2)

    def getFutureInfo(self, id, num):
        if id == 'dt':
            suffix = '_2'
        elif id == 'kt':
            suffix = '_3'
        path = "//li[@id=\"" + num + suffix + "\"]/span[@class=\"IFe3\"]"
        element = self.wd.find_element_by_xpath(path)
        result = element.text
        return result

    def getFutureContract(self, variety):
        contract = self.wd.find_element_by_css_selector('#futures_contract').get_attribute('value')
        self.futures_contract[variety] = 'http://quote.eastmoney.com/qihuo/' + contract + '.html'
        sleep(2)

    def connectToMySQL(self, host, port, user, password, dbname, charset):
        try:
            self.conn = pymysql.connect(host=host,
                                        port=port,
                                        user=user,
                                        password=password,
                                        db=dbname,
                                        charset=charset)
            self.cur = self.conn.cursor()
        except:
            print('连接不成功')
        sql_1 = """
        CREATE TABLE IF NOT EXISTS future_info(
        日期 Date,
        交易所 CHAR(10) NOT NULL,
        期货名称 CHAR(10) NOT NULL,
        多单量 INT,
        空单量 INT
        )ENGINE=innodb DEFAULT CHARSET=utf8;
        """
        sql_2 = """
        CREATE TABLE IF NOT EXISTS variety_info(
        日期 Date,
        品种名称 VARCHAR(10) NOT NULL,
        最新价 INT NOT NULL,
        今开价 INT NOT NULL,
        最高价 INT NOT NULL,
        最低价 INT NOT NULL,
        成交量 VARCHAR(10) NOT NULL,
        持仓量 VARCHAR(10) NOT NULL
        )ENGINE=innodb DEFAULT CHARSET=utf8;
        """
        # 执行SQL语句
        self.cur.execute(sql_1)
        self.cur.execute(sql_2)

    def insertFutureInfo(self):
        for infodict in self.varietyls:
            location = infodict['s1_text']
            variety = infodict['s2_text']
            self.getFutureHTML(infodict)
            self.getFutureContract(variety)
            try:
                search_botton = self.wd.find_element_by_css_selector('[onclick="searchData(false)"]')
                sleep(2)
                search_botton.click()
                sleep(2)
            except selenium.common.exceptions.UnexpectedAlertPresentException:
                continue
            for name, num in self.numdict.items():
                try:
                    multi_quantity = self.getFutureInfo('dt', num)
                except selenium.common.exceptions.NoSuchElementException:
                    multi_quantity = None
                try:
                    empty_quantity = self.getFutureInfo('kt', num)
                except selenium.common.exceptions.NoSuchElementException:
                    empty_quantity = None
                info = [self.date, location, name, multi_quantity, empty_quantity]
                sql = "INSERT INTO future_info(日期, 交易所, 期货名称, 多单量, 空单量) values(%s, %s, %s, %s, %s)"
                self.cur.execute(sql, tuple(info))
                self.conn.commit()
                sleep(2)

    def insertVarietyInfo(self):
        self.futures_contract['郑煤'] = 'http://quote.eastmoney.com/qihuo/zc009.html'
        for variety, link in self.futures_contract.items():
            self.wd.get(link)
            sleep(2)
            zxj = self.wd.find_element_by_css_selector('.zxj').text
            jkj = self.wd.find_element_by_css_selector('.jkj').text
            zgj = self.wd.find_element_by_css_selector('.zgj').text
            zdj = self.wd.find_element_by_css_selector('.zdj').text
            cjl = self.wd.find_element_by_css_selector('.cjl').text
            ccl = self.wd.find_element_by_css_selector('.ccl').text
            info = [self.date, variety, zxj, jkj, zgj, zdj, cjl, ccl]
            sql = "INSERT INTO variety_info(日期, 品种名称, 最新价, 今开价, 最高价, 最低价, 成交量, 持仓量) values(%s, %s, %s, %s, %s, %s, %s, %s)"
            self.cur.execute(sql, tuple(info))
            self.conn.commit()

    def quit(self):
        self.wd.quit()
