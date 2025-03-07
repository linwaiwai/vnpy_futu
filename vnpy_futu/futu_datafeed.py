from datetime import datetime
from typing import List, Optional, Callable
from futu import OpenQuoteContext, KLType, SubType, RET_OK
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.utility import ZoneInfo
from futu.common.sys_config import SysConfig
import os
import pandas as pd
from vnpy.trader.locale import _  # 导入翻译函数

INTERVAL_VT2FUTU = {
    Interval.MINUTE: KLType.K_1M,
    Interval.HOUR: KLType.K_60M,
    Interval.DAILY: KLType.K_DAY,
    Interval.WEEKLY: KLType.K_WEEK,
}

EXCHANGE_VT2FUTU = {
    Exchange.SEHK: "HK",
    Exchange.SMART: "US",
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.NASDAQ: "US",
}

class FutuDatafeed(BaseDatafeed):
    """富途数据服务接口"""
    
    def __init__(self):
        """"""
        self.quote_ctx: Optional[OpenQuoteContext] = None
        self.inited: bool = False
        
    def init(self, setting: dict) -> bool:
        """初始化"""
        host: str = setting["address"]
        port: int = setting["port"]
        is_encrypt: bool = setting.get("is_encrypted", False)
        
        # 处理加密连接
        if is_encrypt:
            # 设置RSA私钥文件路径
            private_key_path = os.path.join(os.path.dirname(__file__), "..", "rsa_key", "rsa_private_key.txt")
            try:
                SysConfig.enable_proto_encrypt(is_encrypt=True)
                SysConfig.set_init_rsa_file(private_key_path)
            except Exception as e:
                print(f"设置加密连接失败：{str(e)}")
                print(f"RSA私钥文件路径: {private_key_path}")
                return False
        
        # 如果已经初始化，先关闭之前的连接
        if self.quote_ctx:
            self.quote_ctx.close()
            
        self.quote_ctx = OpenQuoteContext(host, port, is_encrypt)
        self.inited = True
        print(_("富途数据源初始化成功"))
        return True
    
    def close(self) -> None:
        """关闭连接"""
        if self.quote_ctx:
            self.quote_ctx.close()
        self.inited = False
        
    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> List[BarData]:
        """查询K线数据"""
        if not self.inited:
            output(_("富途数据源未初始化"))
            return []
            
        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval = req.interval
        start: datetime = req.start
        end: datetime = req.end
        
        ktype = INTERVAL_VT2FUTU.get(interval)
        if not ktype:
            output(_("不支持的时间周期: {}").format(interval))
            return []
            
        market = EXCHANGE_VT2FUTU.get(exchange, "")
        if not market:
            output(_("不支持的交易所: {}").format(exchange))
            return []
        
        futu_symbol = f"{market}.{symbol}"
        output(_("查询K线数据: {}, 周期: {}, 开始: {}, 结束: {}").format(futu_symbol, ktype, start, end))
            
        # 查询数据
        ret, data, page_req_key = self.quote_ctx.request_history_kline(
            code=futu_symbol,
            ktype=ktype,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        
        if ret != RET_OK:
            output(_("查询K线数据失败: {}").format(data))
            return []
            
        # 获取所有分页数据
        all_data = data
        while page_req_key:
            ret, data, page_req_key = self.quote_ctx.request_history_kline(
                code=futu_symbol,
                ktype=ktype,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
                page_req_key=page_req_key
            )
            if ret == RET_OK:
                all_data = pd.concat([all_data, data], ignore_index=True)
            else:
                output(_("查询分页数据失败: {}").format(data))
                break
                
        # 整理数据
        bars: List[BarData] = []
        for row in all_data.itertuples():
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=datetime.strptime(row.time_key, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("Asia/Shanghai")),
                interval=interval,
                volume=row.volume,
                turnover=row.turnover,
                open_price=row.open,
                high_price=row.high,
                low_price=row.low,
                close_price=row.close,
                gateway_name="FUTU"
            )
            bars.append(bar)
        
        output(_("获取到 {} 条K线数据").format(len(bars)))    
        return bars 
