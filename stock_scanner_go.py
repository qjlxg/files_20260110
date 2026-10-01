import pandas as pd
from datetime import datetime
import os
import pytz
import glob
from multiprocessing import Pool, cpu_count
import numpy as np

# ==================== 2025“买入即获利”参数调优指南 ===================
# 1. 股价门槛：5.0（默认）。若想做权重大票，调至10.0以上；想博小票弹性，可调至3.0。
MIN_PRICE = 5.0              

# 2. 筹码活跃度：2.5（默认）。数值越小说明筹码锁定越好，越不容易受散户抛压影响。
#    若市场大热，可放宽至2.5，以捕捉活跃题材股。
MAX_AVG_TURNOVER_30 = 5.5    

# --- 极致缩量控制：判断“没人卖了”的关键 ---
# MIN_VOLUME_RATIO：0.2（默认）。防止僵尸股，确保还有起码的流动性。
MIN_VOLUME_RATIO = 0.2       
# MAX_VOLUME_RATIO：1.2（默认）。数值越小，筛选出的股票越“安静”。
# 若想找“刚开始放量”的转折点，可调高至1.5。
MAX_VOLUME_RATIO = 1.2      

# --- 极度超跌控制：判断“跌透了没有” ---
# RSI6_MAX：30（默认）。反映6日内的乖离。调低至25会更严苛，选出的票更少但更安全。
RSI6_MAX = 30                
# KDJ_K_MAX：30（默认）。反映9日内的位置。数值越小，代表股价越贴近近期地板。
KDJ_K_MAX = 30               
# MIN_PROFIT_POTENTIAL：10（默认）。反弹至60日线的理论利润空间。空间越大，胜率相对越低但赔率越高。
MIN_PROFIT_POTENTIAL = 10    

# --- 形态与趋势控制：拒绝追高，只要低吸 ---
# MAX_TODAY_CHANGE：1.5（默认）。限制今日涨幅。
# 调整意义：若市场整体反弹，很多好票首红就超过2%，可调宽至2.5，否则容易漏掉领头羊。
MAX_TODAY_CHANGE = 1.5       
# =====================================================================

SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')
STOCK_DATA_DIR = 'stock_data'
NAME_MAP_FILE = 'stock_names.csv' 

def calculate_indicators(df):
    """计算核心指标"""
    df = df.reset_index(drop=True)
    close = df['收盘']
    
    # 1. RSI6 (计算6日相对强弱)
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=6).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=6).mean()
    rs = gain / loss.replace(0, np.nan)
    df['rsi6'] = 100 - (100 / (1 + rs))
    
    # 2. KDJ (9,3,3) - 标准底部磨底指标
    low_list = df['最低'].rolling(window=9).min()
    high_list = df['最高'].rolling(window=9).max()
    rsv = (df['收盘'] - low_list) / (high_list - low_list) * 100
    df['kdj_k'] = rsv.ewm(com=2).mean()
    
    # 3. 移动平均线 - 用于确认支撑和压力位
    df['ma5'] = close.rolling(window=5).mean()
    df['ma60'] = close.rolling(window=60).mean()
    
    # 4. 换手率与量比 - 用于分析筹码意图
    df['avg_turnover_30'] = df['换手率'].rolling(window=30).mean()
    df['vol_ma5'] = df['成交量'].shift(1).rolling(window=5).mean()
    df['vol_ratio'] = df['成交量'] / df['vol_ma5']
    
    return df

def process_single_stock(args):
    file_path, name_map = args
    stock_code = os.path.basename(file_path).split('.')[0]
    stock_name = name_map.get(stock_code, "未知")
    
    if "ST" in stock_name.upper():
        return None

    try:
        df_raw = pd.read_csv(file_path)
        if len(df_raw) < 60: return None
        
        df = calculate_indicators(df_raw)
        
        # --- 择时哨兵：判断当前是否是最佳介入点 ---
        # 计算截至昨日的连跌天数
        consecutive_drops = (df['收盘'].diff() < 0).astype(int).iloc[:-1][::-1].cumprod().sum()
        
        latest = df.iloc[-1]
        
        # 突破预警：今日收盘价是否超过了前三天的最高价，代表是否有主动买盘进攻
        recent_high = df['最高'].iloc[-4:-1].max() 
        is_breakthrough = "⭐突破" if latest['收盘'] > recent_high else "筑底"
        
        # 量能突变：成交量是否比昨天放大了1.4倍，代表是否有主力入场
        vol_vs_yesterday = latest['成交量'] / df['成交量'].iloc[-2]
        vol_status = "放量" if vol_vs_yesterday > 1.4 else "平量"
        
        # --- 严格执行参数过滤 ---
        if latest['收盘'] < MIN_PRICE or latest['avg_turnover_30'] > MAX_AVG_TURNOVER_30:
            return None
        
        potential = (latest['ma60'] - latest['收盘']) / latest['收盘'] * 100
        change = latest['涨跌幅'] if '涨跌幅' in latest else 0
        if potential < MIN_PROFIT_POTENTIAL or change > MAX_TODAY_CHANGE:
            return None
        
        if latest['rsi6'] > RSI6_MAX or latest['kdj_k'] > KDJ_K_MAX:
            return None
        
        # 核心止跌逻辑：价格不能低于5日均线（拒绝正在阴跌的股票）
        if latest['收盘'] < latest['ma5']:
            return None
            
        if not (MIN_VOLUME_RATIO <= latest['vol_ratio'] <= MAX_VOLUME_RATIO):
            return None

        return {
            '代码': stock_code,
            '名称': stock_name,
            '状态': f"{is_breakthrough}/{vol_status}", 
            '战绩': f"连跌{int(consecutive_drops)}天首红",
            '现价': round(latest['收盘'], 2),
            '今日量比': round(latest['vol_ratio'], 2),
            'RSI6': round(latest['rsi6'], 1),
            '距60日线': f"{round(potential, 1)}%",
            '今日涨跌': f"{round(change, 1)}%"
        }
    except:
        return None

def main():
    now_shanghai = datetime.now(SHANGHAI_TZ)
    print(f"🚀 极致缩量精选扫描开始... (当前时间: {now_shanghai.strftime('%Y-%m-%d %H:%M:%S')})")

    name_map = {}
    if os.path.exists(NAME_MAP_FILE):
        n_df = pd.read_csv(NAME_MAP_FILE, dtype={'code': str})
        name_map = dict(zip(n_df['code'].str.zfill(6), n_df['name']))

    file_list = glob.glob(os.path.join(STOCK_DATA_DIR, '*.csv'))
    tasks = [(file_path, name_map) for file_path in file_list]

    with Pool(processes=cpu_count()) as pool:
        raw_results = pool.map(process_single_stock, tasks)

    results = [r for r in raw_results if r is not None]
        
    if results:
        df_result = pd.DataFrame(results)
        
        # 排序策略：连跌天数越多，且量比越小（筹码越死）的票排在最前面
        df_result['tmp_drops'] = df_result['战绩'].str.extract('(\d+)').astype(int)
        df_result = df_result.sort_values(by=['tmp_drops', '今日量比'], ascending=[False, True])
        df_result = df_result.drop(columns=['tmp_drops'])
        
        print(f"\n🎯 扫描完成！筛选出 {len(results)} 只具备“暴力反弹”潜力的标的:")
        print(df_result.to_string(index=False)) 
        
        # 保存结果到月度目录，方便复盘
        date_str = now_shanghai.strftime('%Y%m%d_%H%M%S')
        year_month = now_shanghai.strftime('%Y/%m')
        save_path = f"results/{year_month}"
        os.makedirs(save_path, exist_ok=True)
        
        file_name = f"极致精选_轮动_{date_str}.csv"
        df_result.to_csv(os.path.join(save_path, file_name), index=False, encoding='utf_8_sig')
        print(f"\n✅ 极精选报告已保存至 {save_path}")
    else:
        print("\n😱 暂无符合条件的极致超跌标的，空仓休息是避开阴跌的最高策略。")

if __name__ == "__main__":
    main()
