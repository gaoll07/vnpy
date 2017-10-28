# encoding: UTF-8
"""
gdax的gateway接入
"""
import json
from datetime import datetime

from vnpy.api.gdax import vngdax
from vnpy.trader.vtGateway import *
from vnpy.trader.vtFunction import getJsonPath
from vnpy.trader.language.chinese.constant import *

DIRECTION_MAP = {}
DIRECTION_MAP['buy'] = DIRECTION_LONG
DIRECTION_MAP['sell'] = DIRECTION_SHORT

STATUS_MAP = {}
STATUS_MAP[0] = STATUS_NOTTRADED
STATUS_MAP[1] = STATUS_PARTTRADED
STATUS_MAP[2] = STATUS_ALLTRADED
STATUS_MAP[3] = STATUS_CANCELLED
STATUS_MAP[5] = STATUS_UNKNOWN
STATUS_MAP[7] = STATUS_UNKNOWN


class GdaxGateway(VtGateway):
    """GDAX接口"""

    def __init__(self, eventEngine, gatewayName='GDAX'):
        """Constructor"""
        super(GdaxGateway, self).__init__(eventEngine, gatewayName)

        self.tradeApi = GdaxTradeApi(self)
        self.dataApi = GdaxDataApi(self)

        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)

    def connect(self):
        """连接"""
        # 载入json文件
        try:
            f = file(self.filePath)
        except IOError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = u'读取连接配置出错，请检查'
            self.onLog(log)
            return

        # 解析json文件
        setting = json.load(f)
        try:
            accessKey = str(setting['accessKey'])
            secretKey = str(setting['secretKey'])
            passPhrase = str(setting['passPhrase'])
            interval = setting['interval']
            debug = setting['debug']
        except KeyError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = u'连接配置缺少字段，请检查'
            self.onLog(log)
            return

        # 初始化接口
        self.tradeApi.connect(accessKey, secretKey, passPhrase, debug)
        self.writeLog(u'交易接口初始化成功')

        self.dataApi.connect(interval, debug)
        self.writeLog(u'行情接口初始化成功')

        # 启动查询
        self.initQuery()
        self.startQuery()

    def writeLog(self, content):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.onLog(log)

    def subscribe(self, subscribeReq):
        """订阅行情"""
        raise NotImplementedError

    def sendOrder(self, orderReq):
        """发单"""
        self.tradeApi.sendOrder(orderReq)

    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        self.tradeApi.cancel(cancelOrderReq)

    def qryAccount(self):
        """查询账户资金"""
        self.tradeApi.queryAccount()

    def qryPosition(self):
        """查询持仓"""
        raise NotImplementedError

    def close(self):
        """关闭"""
        self.tradeApi.exit()
        self.dataApi.exit()

    def initQuery(self):
        """初始化连续查询"""
        if self.qryEnabled:
            self.qryFunctionList = [self.tradeApi.queryWorkingOrders, self.tradeApi.queryAccount]
            self.startQuery()

    def query(self, event):
        """注册到事件处理引擎上的查询函数"""
        for function in self.qryFunctionList:
            function()

    def startQuery(self):
        """启动连续查询"""
        self.eventEngine.register(EVENT_TIMER, self.query)

    def setQryEnabled(self, qryEnabled):
        """设置是否要启动循环查询"""
        self.qryEnabled = qryEnabled


class GdaxTradeApi(vngdax.TradeApi):
    """交易接口"""

    def __init__(self, gateway):
        """Constructor"""
        super(GdaxTradeApi, self).__init__()

        self.gateway = gateway
        self.gatewayName = gateway.gatewayName

        self.localID = 0            # 本地委托号
        self.localSystemDict = {}   # key:localID, value:systemID
        self.systemLocalDict = {}   # key:systemID, value:localID
        self.workingOrderDict = {}  # key:localID, value:order
        self.reqLocalDict = {}      # key:reqID, value:localID
        self.cancelDict = {}        # key:localID, value:cancelOrderReq

        self.tradeID = 0            # 本地成交号

    def onError(self, error, req, reqID):
        """错误推送"""
        err = VtErrorData()
        err.gatewayName = self.gatewayName
        err.errorMsg = str(error)
        err.errorTime = datetime.now().strftime('%H:%M:%S')
        self.gateway.onError(err)

    def onGetAccountInfo(self, data, req, reqID):
        """查询账户回调
        
        Parameters
        ----------
        data : list
            returned by gdax_client.get_accounts()
        """
        # 推送账户数据
        for account_data in data:
            account = VtAccountData()
            account.gatewayName = self.gatewayName
            account.accountID = 'GDAX#{}'.format(account_data['currency'])
            account.vtAccountID = '.'.join([account.accountID, self.gatewayName])
            account.available = float(account_data['available'])
            account.balance = float(account_data['balance'])
            account.currency = account_data['currency']
            account.id = account_data['id']
            account.profile_id = account_data['profile_id']
            self.gateway.onAccount(account)

            # 推送持仓数据
            pos = VtPositionData()
            pos.gatewayName = self.gatewayName
            pos.symbol = account_data['currency']
            pos.exchange = EXCHANGE_GDAX
            pos.vtSymbol = '.'.join([pos.symbol, pos.exchange])
            pos.vtPositionName = pos.vtSymbol
            pos.position = float(account_data['balance'])
            self.gateway.onPosition(pos)

    def onGetOrders(self, data, req, reqID):
        """查询委托回调"""
        for d in data:
            order = VtOrderData()
            order.gatewayName = self.gatewayName

            # 合约代码
            order.symbol = d['product_id']
            order.exchange = EXCHANGE_GDAX
            order.vtSymbol = d['product_id']

            # 委托号
            systemID = d['id']
            self.localID += 1
            localID = str(self.localID)
            self.systemLocalDict[systemID] = localID
            self.localSystemDict[localID] = systemID
            order.orderID = localID
            order.vtOrderID = '.'.join([order.orderID, order.gatewayName])

            # 其他信息
            order.direction = DIRECTION_MAP[d['side']]
            order.offset = OFFSET_NONE
            order.price = float(d['price'])
            order.totalVolume = float(d['size'])
            order.tradedVolume = float(d['filled_size'])
            order.orderTime = d['created_at']
            order.status = d['status']

            # 委托状态
            if order.tradedVolume == 0:
                order.status = STATUS_NOTTRADED
            else:
                order.status = STATUS_PARTTRADED

            # 缓存病推送
            self.workingOrderDict[localID] = order
            self.gateway.onOrder(order)

    def onOrderInfo(self, data, req, reqID):
        """委托详情回调"""
        systemID = data['id']
        localID = self.systemLocalDict[systemID]
        order = self.workingOrderDict.get(localID, None)
        if not order:
            return

        # 记录最新成交的金额
        newTradeVolume = float(data['processed_amount']) - order.tradedVolume
        if newTradeVolume:
            trade = VtTradeData()
            trade.gatewayName = self.gatewayName
            trade.symbol = order.symbol
            trade.vtSymbol = order.vtSymbol

            self.tradeID += 1
            trade.tradeID = str(self.tradeID)
            trade.vtTradeID = '.'.join([trade.tradeID, trade.gatewayName])

            trade.volume = newTradeVolume
            trade.price = data['processed_price']
            trade.direction = order.direction
            trade.offset = order.offset
            trade.exchange = order.exchange
            trade.tradeTime = datetime.now().strftime('%H:%M:%S')

            self.gateway.onTrade(trade)

        # 更新委托状态
        order.tradedVolume = float(data['processed_amount'])
        order.status = STATUS_MAP.get(data['status'], STATUS_UNKNOWN)

        if newTradeVolume:
            self.gateway.onOrder(order)

        if order.status == STATUS_ALLTRADED or order.status == STATUS_CANCELLED:
            del self.workingOrderDict[order.orderID]

    def onBuy(self, data, req, reqID):
        """买入回调"""
        localID = self.reqLocalDict[reqID]
        systemID = data['id']
        self.localSystemDict[localID] = systemID
        self.systemLocalDict[systemID] = localID

        # 撤单
        if localID in self.cancelDict:
            req = self.cancelDict[localID]
            self.cancel(req)
            del self.cancelDict[localID]

        # 推送委托信息
        order = self.workingOrderDict[localID]
        if data['status'] == 'success':
            order.status = STATUS_NOTTRADED
        self.gateway.onOrder(order)

    def onSell(self, data, req, reqID):
        """卖出回调"""
        localID = self.reqLocalDict[reqID]
        systemID = data['id']
        self.localSystemDict[localID] = systemID
        self.systemLocalDict[systemID] = localID

        # 撤单
        if localID in self.cancelDict:
            req = self.cancelDict[localID]
            self.cancel(req)
            del self.cancelDict[localID]

        # 推送委托信息
        order = self.workingOrderDict[localID]
        if data['result'] == 'success':
            order.status = STATUS_NOTTRADED
        self.gateway.onOrder(order)

    def onBuyMarket(self, data, req, reqID):
        """市价买入回调"""
        print data

    def onSellMarket(self, data, req, reqID):
        """市价卖出回调"""
        print data

    def onCancelOrder(self, data, req, reqID):
        """撤单回调"""
        if data['status'] == 'success':
            systemID = req['params']['id']
            localID = self.systemLocalDict[systemID]

            order = self.workingOrderDict[localID]
            order.status = STATUS_CANCELLED

            del self.workingOrderDict[localID]
            self.gateway.onOrder(order)

    def onGetNewDealOrders(self, data, req, reqID):
        """查询最新成交回调"""
        print data

    def onGetOrderIdByTradeId(self, data, req, reqID):
        """通过成交编号查询委托编号回调"""
        print data

    def onWithdrawCoin(self, data, req, reqID):
        """提币回调"""
        print data

    def onCancelWithdrawCoin(self, data, req, reqID):
        """取消提币回调"""
        print data

    def onGetWithdrawCoinResult(self, data, req, reqID):
        """查询提币结果回调"""
        print data

    def onTransfer(self, data, req, reqID):
        """转账回调"""
        print data

    def onLoan(self, data, req, reqID):
        """申请杠杆回调"""
        print data

    def onRepayment(self, data, req, reqID):
        """归还杠杆回调"""
        print data

    def onLoanAvailable(self, data, req, reqID):
        """查询杠杆额度回调"""
        print data

    def onGetLoans(self, data, req, reqID):
        """查询杠杆列表"""
        print data

    def connect(self, accessKey, secretKey, passPhrase, debug=False):
        """连接服务器"""
        self.DEBUG = debug

        self.init(accessKey, secretKey, passPhrase)

        # 查询未成交委托
        self.getOrders()
        self.queryAccount()

    def queryWorkingOrders(self):
        """查询活动委托状态"""
        for order in self.workingOrderDict.values():
            # 如果尚未返回委托号，则无法查询
            if order.orderID in self.localSystemDict:
                systemID = self.localSystemDict[order.orderID]
                self.getOrder(systemID)

    def queryAccount(self):
        """查询活动委托状态"""
        self.getAccountInfo()

    def sendOrder(self, req):
        """发送委托"""
        # 检查是否填入了价格，禁止市价委托
        if req.priceType != PRICETYPE_LIMITPRICE:
            err = VtErrorData()
            err.gatewayName = self.gatewayName
            err.errorMsg = u'GDAX接口仅支持限价单'
            err.errorTime = datetime.now().strftime('%H:%M:%S')
            self.gateway.onError(err)
            return None

        # 发送限价委托
        if req.direction == DIRECTION_LONG:
            reqID = self.buy(req.price, req.volume, req.symbol)
        else:
            reqID = self.sell(req.price, req.volume, req.symbol)

        self.localID += 1
        localID = str(self.localID)
        self.reqLocalDict[reqID] = localID

        # 推送委托信息
        order = VtOrderData()
        order.gatewayName = self.gatewayName

        order.symbol = req.symbol
        order.exchange = EXCHANGE_GDAX
        order.vtSymbol = '.'.join([order.symbol, order.exchange])

        order.orderID = localID
        order.vtOrderID = '.'.join([order.orderID, order.gatewayName])

        order.direction = req.direction
        order.offset = OFFSET_UNKNOWN
        order.price = req.price
        order.volume = req.volume
        order.orderTime = datetime.now().strftime('%H:%M:%S')
        order.status = STATUS_UNKNOWN

        self.workingOrderDict[localID] = order
        self.gateway.onOrder(order)

        # 返回委托号
        return order.vtOrderID

    def cancel(self, req):
        """撤单"""
        localID = req.orderID
        if localID in self.localSystemDict:
            systemID = self.localSystemDict[localID]
            # coin, market = req.symbol.split('.')
            self.cancelOrder(systemID)
        else:
            self.cancelDict[localID] = req


class GdaxDataApi(vngdax.DataApi):
    """行情接口"""

    def __init__(self, gateway):
        """Constructor"""
        super(GdaxDataApi, self).__init__()

        self.gateway = gateway
        self.gatewayName = gateway.gatewayName

        self.tickDict = {}      # key:symbol, value:tick

    def onTick(self, data):
        """实时成交推送"""
        raise NotImplementedError

    def onQuote(self, data):
        """实时报价推送"""
        symbol = data['symbol']

        if symbol not in self.tickDict:
            tick = VtTickData()
            tick.gatewayName = self.gatewayName

            tick.symbol = symbol
            tick.exchange = EXCHANGE_GDAX
            tick.vtSymbol = '.'.join([tick.symbol, tick.exchange])
            self.tickDict[symbol] = tick
        else:
            tick = self.tickDict[symbol]

        tick.lastPrice = float(data['price'])
        tick.lastVolume = float(data['size'])
        tick.volume = float(data['volume'])
        tick.bidPrice1 = float(data['bid'])
        tick.askPrice1 = float(data['ask'])
        tick.time = data['time']

    def onDepth(self, data):
        """实时深度推送"""
        symbol = data['symbol']

        if symbol not in self.tickDict:
            tick = VtTickData()
            tick.gatewayName = self.gatewayName

            tick.symbol = symbol
            tick.exchange = EXCHANGE_GDAX
            tick.vtSymbol = '.'.join([tick.symbol, tick.exchange])
            self.tickDict[symbol] = tick
        else:
            tick = self.tickDict[symbol]

        tick.bidPrice1, tick.bidVolume1 = float(data['bids'][0][0]), float(data['bids'][0][1])
        tick.bidPrice2, tick.bidVolume2 = float(data['bids'][1][0]), float(data['bids'][0][1])
        tick.bidPrice3, tick.bidVolume3 = float(data['bids'][2][0]), float(data['bids'][0][1])
        tick.bidPrice4, tick.bidVolume4 = float(data['bids'][3][0]), float(data['bids'][0][1])
        tick.bidPrice5, tick.bidVolume5 = float(data['bids'][4][0]), float(data['bids'][0][1])

        tick.askPrice1, tick.askVolume1 = float(data['asks'][0][0]), float(data['bids'][0][1])
        tick.askPrice2, tick.askVolume2 = float(data['asks'][1][0]), float(data['bids'][0][1])
        tick.askPrice3, tick.askVolume3 = float(data['asks'][2][0]), float(data['bids'][0][1])
        tick.askPrice4, tick.askVolume4 = float(data['asks'][3][0]), float(data['bids'][0][1])
        tick.askPrice5, tick.askVolume5 = float(data['asks'][4][0]), float(data['bids'][0][1])

        now = datetime.now()
        tick.time = now.strftime('%H:%M:%S')
        tick.date = now.strftime('%Y%m%d')

        self.gateway.onTick(tick)

    def connect(self, interval, debug=False):
        """连接服务器"""
        self.init(interval, debug)

        # 订阅行情并推送合约信息
        # TODO: add other contracts
        for s in vngdax.SYMBOL:
            self.subscribeQuote(s)
            self.subscribeDepth(s)

            contract = VtContractData()
            contract.gatewayName = self.gatewayName
            contract.symbol = s
            contract.exchange = EXCHANGE_GDAX
            contract.vtSymbol = '.'.join([contract.symbol, contract.exchange])
            contract.name = s
            contract.size = 1
            contract.priceTick = 0.01
            contract.productClass = PRODUCT_SPOT
            self.gateway.onContract(contract)


if __name__ == '__main__':
    from vnpy.event.eventEngine import EventEngine
    ee = EventEngine()
    gateway = GdaxGateway(ee)
    trade_api = GdaxTradeApi(gateway)
    gateway.connect()
    data_api = GdaxDataApi(gateway)
    data_api.connect(interval=5)