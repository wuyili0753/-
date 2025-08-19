import akshare as ak  # 替换yfinance，专为国内金融数据设计
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import time
import warnings
warnings.filterwarnings('ignore')

# ================== 策略配置 ==================
ETF_MAP = {
    '510720': '上证红利ETF',  # 国内ETF代码无需加.SS后缀
    '515180': '中证红利ETF', 
    '512890': '红利低波ETF'
}

DEFAULT_DIV = {
    '510720': 4.5,
    '515180': 4.2,
    '512890': 4.0
}

LOOKBACK_DAYS = 60      # 动量观察期
HOLDING_DAYS = 20       # 持仓周期
RISK_FREE_RATE = 0.02   # 无风险利率
STOP_LOSS = -0.08       # 止损线
BATCH_SIZE = 3          # 分批建仓次数
DIV_CACHE_DAYS = 30     # 股息率缓存天数
REQUEST_DELAY = 2       # 请求间隔（秒）

# ================== 股息率获取函数 ==================
def get_dividend_rates():
    """通过东方财富获取国内ETF股息率"""
    cache_file = 'dividend_cache.json'
    
    if os.path.exists(cache_file):
        with open(cache_file, 'r', encoding='utf-8') as f:
            try:
                cache = json.load(f)
                last_update = datetime.strptime(cache['last_update'], '%Y-%m-%d')
                if (datetime.now() - last_update).days < DIV_CACHE_DAYS:
                    print("使用缓存的股息率数据")
                    return cache['rates']
            except:
                pass
    
    print("开始获取最新股息率...")
    div_rates = {}
    
    for ticker in ETF_MAP:
        try:
            # 东方财富ETF概况接口（国内数据更准确）
            fund_profile = ak.fund_etf_profile_em(symbol=ticker)
            # 提取股息率（处理中文描述）
            div_row = fund_profile[fund_profile['item'] == '股息率']
            if not div_row.empty:
                div_value = div_row['value'].iloc[0]
                if '%' in div_value:
                    div_rate = float(div_value.strip('%'))
                    if 0 < div_rate < 15:
                        div_rates[ticker] = round(div_rate, 2)
                        print(f"  {ETF_MAP[ticker]} 股息率: {div_rate}%")
                        continue
            
            raise ValueError("未找到有效股息率")
                
        except Exception as e:
            print(f"  {ETF_MAP[ticker]} 股息率获取失败，使用默认值: {str(e)}")
            div_rates[ticker] = DEFAULT_DIV.get(ticker, 3.0)
        
        time.sleep(REQUEST_DELAY)
    
    # 保存缓存
    cache_data = {
        'last_update': datetime.now().strftime('%Y-%m-%d'),
        'rates': div_rates
    }
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2, ensure_ascii=False)
    
    return div_rates

# ================== 数据获取 ==================
def fetch_etf_data():
    """获取国内ETF价格数据（兼容最新数据源格式）"""
    etf_data = {}
    end_date = datetime.now().strftime('%Y%m%d')  # 注意格式修改为纯数字
    start_date = (datetime.now() - timedelta(days=365*2)).strftime('%Y%m%d')  # 2年数据
    
    for ticker in ETF_MAP:
        try:
            print(f"获取 {ETF_MAP[ticker]}({ticker}) 价格数据...")
            # 使用东方财富ETF日线接口（调整参数和列名适配）
            df = ak.fund_etf_hist_em(
                symbol=ticker,
                start_date=start_date,
                end_date=end_date,
                period="daily"
            )
            
            # 打印调试信息（查看实际返回的列名）
            print(f"  数据源返回列名: {df.columns.tolist()}")
            
            # 灵活匹配列名（处理可能的列名变化）
            date_cols = [col for col in df.columns if '日期' in col]
            close_cols = [col for col in df.columns if '收盘' in col]
            
            if not date_cols or not close_cols:
                raise ValueError("未找到日期或收盘价列")
            
            # 重命名为标准化列名
            df = df.rename(columns={
                date_cols[0]: 'date',
                close_cols[0]: 'close',
            })
            
            # 确保包含必要的价格列
            required_cols = ['date', 'close']
            if not all(col in df.columns for col in required_cols):
                raise ValueError(f"缺少必要列: {required_cols}")
            
            # 转换日期格式并排序
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').set_index('date')
            
            # 检查数据量
            if len(df) < LOOKBACK_DAYS:
                raise ValueError(f"数据不足{LOOKBACK_DAYS}条（仅获取{len(df)}条）")
            
            etf_data[ticker] = df
            print(f"  成功获取 {len(df)} 条数据")
            
        except Exception as e:
            print(f"  {ETF_MAP[ticker]} 价格数据获取失败: {str(e)}")
        
        time.sleep(2)  # 延长请求间隔，避免被限制
    
    return etf_data

# ================== 指标计算 ==================
def calculate_signals(data_dict, div_rates):
    """计算交易信号"""
    signals = pd.DataFrame()
    
    for ticker, df in data_dict.items():
        if len(df) < LOOKBACK_DAYS:
            continue
            
        returns = np.log(df['close'] / df['close'].shift(1))
        momentum = df['close'].pct_change(periods=LOOKBACK_DAYS)
        volatility = returns.rolling(LOOKBACK_DAYS).std() * np.sqrt(252)
        
        # 处理波动率为0的特殊情况
        if volatility.iloc[-1] == 0:
            sharpe = momentum.iloc[-1] * 10  # 简单替代
        else:
            sharpe = (momentum - RISK_FREE_RATE/252) / volatility
        
        div_factor = div_rates.get(ticker, 3.0) / 100
        score = sharpe * 0.7 + div_factor * 0.3
        
        signals[ticker] = score
    
    signals.dropna(inplace=True)
    return signals

# ================== 主函数 ==================
if __name__ == "__main__":
    print("红利ETF轮动策略 (国内数据源版)")
    
    # 1. 获取股息率数据
    div_rates = get_dividend_rates()
    
    # 2. 获取价格数据
    print("\n获取ETF价格数据...")
    etf_data = fetch_etf_data()
    
    if not etf_data:
        print("价格数据获取失败，无法继续")
    else:
        # 3. 计算信号
        signals = calculate_signals(etf_data, div_rates)
        
        if not signals.empty:
            print("\n最新信号（分数越高越优）:")
            print(signals.tail(1).T.sort_values(by=signals.index[-1], ascending=False))
            
            # 输出推荐标的
            best_ticker = signals.iloc[-1].idxmax()
            print(f"\n当前推荐标的: {ETF_MAP[best_ticker]}({best_ticker})")
        else:
            print("无法生成有效信号")
            import requests

def send_wechat_notification(message):
    """通过 Server酱 发送微信通知"""
    sendkey = "你的_Server酱_SendKey"  # 替换成你的 SendKey（后面会讲如何获取）
    url = f"https://sctapi.ftqq.com/{sendkey}.send"
    data = {
        "text": "红利三剑客",  # 微信消息标题
        "desp": message           # 微信消息内容（详细信息）
    }
    try:
        response = requests.post(url, data=data)
        print("微信通知发送成功:", response.text)
    except Exception as e:
        print("微信通知发送失败:", str(e))

# 在选股完成后调用（示例）
if 'selected_etf' in locals() and 'last_signal' in locals():
    selected_name = ETF_CONFIG[selected_etf]['name']
    message = f"""
    📈 **今日ETF轮动结果**
    - **推荐ETF**: {selected_name} ({selected_etf})
    - **信号强度**: {last_signal[selected_etf]:.4f}
    - **股息率**: {div_rates.get(selected_etf, 'N/A')}%
    - **运行时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    send_wechat_notification(message)
else:
    send_wechat_notification("❌ 策略运行失败，请检查日志！")