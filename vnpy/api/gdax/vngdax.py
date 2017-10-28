# encoding: utf-8


import urllib
import hashlib

import requests
from time import time, sleep
from Queue import Queue, Empty
from threading import Thread

import gdax


# 常量定义
COINTYPE_BTC = 1
COINTYPE_ETH = 2
COINTYPE_LTC = 3

LOANTYPE_USD = 1

SYMBOL = [
    'BTC-EUR',
    'BTC-GBP',
    'BTC-USD',
    'ETH-BTC',
    'ETH-EUR',
    'ETH-USD',
    'LTC-BTC',
    'LTC-EUR',
    'LTC-USD',
]

MARKETTYPE_USD = 'usd'

FUNCTIONCODE_GETACCOUNTS = 'get_accounts'
FUNCTIONCODE_GETACCOUNT = 'get_account'
FUNCTIONCODE_GETACCOUNTHISTORY = 'get_account_history'
FUNCTIONCODE_GETACCOUNTHOLDS = 'get_account_holds'
FUNCTIONCODE_BUY = 'buy'
FUNCTIONCODE_SELL = 'sell'
FUNCTIONCODE_CANCELORDER = 'cancel_order'
FUNCTIONCODE_CANCELALL = 'cancel_all'
FUNCTIONCODE_GETORDERS = 'get_orders'
FUNCTIONCODE_GETORDER = 'get_order'
FUNCTIONCODE_GETFILLS = 'get_fills'
FUNCTIONCODE_DEPOSIT = 'deposit'
FUNCTIONCODE_WITHDRAW = 'withdraw'

# API 相关定义
GDAX_API = 'https://api.gdax.com'
GDAX_SANDBOX_API = 'https://api-public.sandbox.gdax.com'


def signature(params):
    """生成签名"""
    params = sorted(params.iteritems(), key=lambda d: d[0], reverse=False)
    message = urllib.urlencode(params)

    m = hashlib.md5()
    m.update(message)
    m.digest()

    sig = m.hexdigest()
    return sig


class TradeApi(object):
    """交易接口"""
    DEBUG = True

    def __init__(self):
        """Constructor"""
        self.accessKey = ''
        self.secretKey = ''
        self.passPhrase = ''
        self.authClient = None

        self.active = False         # API工作状态
        self.reqID = 0              # 请求编号
        self.reqQueue = Queue()     # 请求队列
        self.reqThread = Thread(target=self.processQueue)   # 请求处理线程

    def processRequest(self, req):
        """处理请求"""
        # 读取方法和参数
        method = req['method']
        params = req['params']
        optional = req['optional']

        # 添加选填参数
        if optional:
            params.update(optional)

        func = eval('self.authClient.{}'.format(method))
        data = func(**params)
        # I do not know why get_orders returns a list of list.
        if method == 'get_orders':
            return data[0]
        else:
            return data

    def processQueue(self):
        """处理请求队列中的请求"""
        while self.active:
            try:
                req = self.reqQueue.get(block=True, timeout=1)  # 获取请求的阻塞为一秒
                callback = req['callback']
                reqID = req['reqID']

                data = self.processRequest(req)

                # 请求失败
                if 'message' in data:
                    error = u'错误信息：%s' % data['message']
                    self.onError(error, req, reqID)
                # 请求成功
                else:
                    if self.DEBUG:
                        print callback.__name__
                    callback(data, req, reqID)

            except Empty:
                pass

    def sendRequest(self, method, params, callback, optional=None):
        """发送请求"""
        # 请求编号加1
        self.reqID += 1

        # 生成请求字典并放入队列中
        req = {}
        req['method'] = method
        req['params'] = params
        req['callback'] = callback
        req['optional'] = optional
        req['reqID'] = self.reqID
        self.reqQueue.put(req)

        # 返回请求编号
        return self.reqID

    ####################################################
    ## 主动函数
    ####################################################

    def init(self, accessKey, secretKey, passPhrase):
        """初始化"""
        self.accessKey = accessKey
        self.secretKey = secretKey
        self.passPhrase = passPhrase

        self.active = True
        self.reqThread.start()
        self.authClient = gdax.AuthenticatedClient(
            self.accessKey, self.secretKey, self.passPhrase, api_url=GDAX_SANDBOX_API)

    def exit(self):
        """退出"""
        self.active = False

        if self.reqThread.isAlive():
            self.reqThread.join()

    def getAccountInfo(self):
        """查询账户"""
        method = FUNCTIONCODE_GETACCOUNTS
        params = {}
        callback = self.onGetAccountInfo
        return self.sendRequest(method, params, callback)

    def getOrders(self):
        """查询委托"""
        method = FUNCTIONCODE_GETORDERS
        params = {}
        callback = self.onGetOrders
        return self.sendRequest(method, params, callback)

    def getOrder(self, order_id):
        """获取单个委托"""
        method = FUNCTIONCODE_GETORDER
        params = {'order_id': order_id}
        callback = self.onGetOrder
        return self.sendRequest(method, params, callback)

    def getFills(self, order_id=None, product_id=None):
        """查询成交委托"""
        method = FUNCTIONCODE_GETFILLS
        params = {'order_id': order_id, 'product_id': product_id}
        callback = self.onGetFills
        return self.sendRequest(method, params, callback)

    def buy(self, price, size, product_id):
        """委托买入"""
        method = FUNCTIONCODE_BUY
        params = {
            'price': price,
            'size': size,
            'product_id': product_id
        }
        callback = self.onBuy
        return self.sendRequest(method, params, callback)

    def sell(self, price, size, product_id):
        """委托卖出"""
        method = FUNCTIONCODE_SELL
        params = {
            'price': price,
            'size': size,
            'product_id': product_id
        }
        callback = self.onSell
        return self.sendRequest(method, params, callback)

    def cancelOrder(self, order_id):
        """撤销委托"""
        method = FUNCTIONCODE_CANCELORDER
        params = {'order_id': order_id}
        callback = self.onCancelOrder
        return self.sendRequest(method, params, callback)

    def cancelAll(self, product_id):
        """查询最新10条成交"""
        method = FUNCTIONCODE_CANCELALL
        params = {'product_id': product_id}
        callback = self.onCancelAll
        return self.sendRequest(method, params, callback)

    def withdraw(self, amount, coinbase_account_id):
        """提取"""
        method = FUNCTIONCODE_WITHDRAW
        params = {
            'amount': amount,
            'coinbase_account_id': coinbase_account_id
        }
        callback = self.onWithdraw
        return self.sendRequest(method, params, callback)

    def deposit(self, amount, coinbase_account_id):
        """储蓄"""
        method = FUNCTIONCODE_DEPOSIT
        params = {
            'amount': amount,
            'coinbase_account_id': coinbase_account_id
        }
        callback = self.onDeposit
        return self.sendRequest(method, params, callback)

    ####################################################
    ## 回调函数
    ####################################################

    def onError(self, error, req, reqID):
        """错误推送"""
        print error, reqID

    def onGetAccountInfo(self, data, req, reqID):
        """查询账户回调"""
        raise NotImplementedError

    def onGetOrders(self, data, req, reqID):
        """查询委托回调"""
        raise NotImplementedError

    def onGetOrder(self, data, req, reqID):
        """查询单个委托回调"""
        pass

    def onGetFills(self, data, req, reqID):
        """委托成交回调"""
        raise NotImplementedError

    def onBuy(self, data, req, reqID):
        """买入回调"""
        raise NotImplementedError

    def onSell(self, data, req, reqID):
        """卖出回调"""
        raise NotImplementedError

    def onCancelOrder(self, data, req, reqID):
        """撤单回调"""
        raise NotImplementedError

    def onCancelAll(self, data, req, reqID):
        """全部撤单回调"""
        raise NotImplementedError

    def onWithdraw(self, data, req, reqID):
        """储蓄回调"""
        raise NotImplementedError

    def onDeposit(self, data, req, reqID):
        """提取回调"""
        raise NotImplementedError


class DataApi(object):
    """行情接口"""

    DEBUG = True

    TICK_SYMBOL_URL = {s: '{}/products/{}/ticker'.format(
        GDAX_API, s) for s in SYMBOL}

    QUOTE_SYMBOL_URL = {s: '{}/products/{}/book'.format(
        GDAX_API, s) for s in SYMBOL}

    DEPTH_SYMBOL_URL = {key: url + '/?level=2' for key, url in QUOTE_SYMBOL_URL.iteritems()}

    def __init__(self):
        """Constructor"""
        self.active = False

        self.taskInterval = 0                       # 每轮请求延时
        self.taskList = []                          # 订阅的任务列表
        self.taskThread = Thread(target=self.run)   # 处理任务的线程

    def init(self, interval, debug):
        """初始化"""
        self.taskInterval = interval
        self.DEBUG = debug

        self.active = True
        self.taskThread.start()

    def exit(self):
        """退出"""
        self.active = False

        if self.taskThread.isAlive():
            self.taskThread.join()

    def run(self):
        """连续运行"""
        while self.active:
            for symbol, url, callback in self.taskList:
                try:
                    r = requests.get(url)
                    if r.status_code == 200:
                        data = r.json()
                        data.update({'symbol': symbol})
                        if self.DEBUG:
                            print callback.__name__
                        callback(data)
                except Exception, e:
                    print e

            sleep(self.taskInterval)

    def subscribeTick(self, symbol):
        """订阅实时成交数据"""
        url = self.TICK_SYMBOL_URL[symbol]
        task = (symbol, url, self.onTick)
        self.taskList.append(task)

    # TODO: Need to change.
    # The current implimentation uses different data from Huobi.
    def subscribeQuote(self, symbol):
        """订阅实时报价数据"""
        url = self.TICK_SYMBOL_URL[symbol]
        task = (symbol, url, self.onQuote)
        self.taskList.append(task)

    def subscribeDepth(self, symbol, level=0):
        """订阅深度数据"""
        url = self.DEPTH_SYMBOL_URL[symbol].format(level)
        task = (symbol, url, self.onDepth)
        self.taskList.append(task)

    def onTick(self, data):
        """实时成交推送"""
        raise NotImplementedError

    def onQuote(self, data):
        """实时报价推送"""
        raise NotImplementedError

    def onDepth(self, data):
        """实时深度推送"""
        raise NotImplementedError

    def getKline(self, symbol, period, length=0):
        """查询K线数据"""
        raise NotImplementedError