import pandas as pd
import os
from datetime import datetime
import multiprocessing

# 技术参数定义
MA_FAST = 13
MA_SLOW = 55
VMA_WINDOW = 5        
LIMIT_UP_THRESHOLD = 9.8  
LOOKBACK_WINDOW = 6       

def analyze_stock(file_path, names_dict):
    try:
        # 读取CSV
        df = pd.read_csv(file_path, encoding='utf-8')
        if len(df) < MA_SLOW + VMA_WINDOW:
            return None
        
        # 确保按时间升序
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        
        # --- 基础属性过滤 ---
        raw_code = str(df.iloc[-1]['股票代码']).split('.')[0].zfill(6)
        name = names_dict.get(raw_code, "未知名称")
        
        # 排除 ST 和 退市股
        if any(keyword in name for keyword in ['ST', '退', '*']):
            return None

        # --- 技术指标计算 ---
        df['MA13'] = df['收盘'].rolling(window=MA_FAST).mean()
        df['MA55'] = df['收盘'].rolling(window=MA_SLOW).mean()
        df['VMA5'] = df['成交量'].rolling(window=VMA_WINDOW).mean()
        
        # 获取切片
        recent_df = df.tail(LOOKBACK_WINDOW)
        current_day = recent_df.iloc[-1]
        previous_days = recent_df.iloc[:-1]
        yesterday = previous_days.iloc[-1]
        
        # --- 核心筛选逻辑 ---
        # 1. 前几日有过涨停
        has_recent_limit_up = any(previous_days['涨跌幅'] >= LIMIT_UP_THRESHOLD)
        
        # 2. 趋势与支撑：均线多头且回踩 MA13 (浮动1%空间)
        on_support = current_day['MA13'] > current_day['MA55'] and \
                     current_day['收盘'] >= current_day['MA13'] * 0.99
        
        # 3. 前期缩量：昨日成交量小于前几日最大成交量的 70% (洗盘信号)
        max_vol_recent = previous_days['成交量'].max()
        is_shrinking_vol = yesterday['成交量'] < max_vol_recent * 0.7
        
        # 4. 买点确认：今日放量阳线
        is_positive_candle = current_day['涨跌幅'] > 1.0
        is_volume_rebound = current_day['成交量'] > yesterday['成交量'] and \
                            current_day['成交量'] > current_day['VMA5']
        
        if has_recent_limit_up and on_support and is_shrinking_vol and is_positive_candle and is_volume_rebound:
            return {
                "代码": raw_code,
                "名称": name,
                "日期": current_day['日期'].strftime('%Y-%m-%d'),
                "收盘价": current_day['收盘'],
                "涨跌幅_数值": current_day['涨跌幅'],
                "涨跌幅": f"{current_day['涨跌幅']}%",
                "成交量比VMA5": round(current_day['成交量'] / current_day['VMA5'], 2),
                "换手率": current_day['换手率']
            }
    except:
        return None
    return None

def main():
    stock_data_dir = './stock_data'
    
    try:
        names_df = pd.read_csv('stock_names.csv', dtype={'code': str})
        names_dict = dict(zip(names_df['code'].str.zfill(6), names_df['name']))
    except:
        names_dict = {}

    if not os.path.exists(stock_data_dir):
        print(f"错误：目录 {stock_data_dir} 不存在")
        return

    files = [os.path.join(stock_data_dir, f) for f in os.listdir(stock_data_dir) if f.endswith('.csv')]
    print(f"🚀 正在过滤并扫描 {len(files)} 只个股...")
    
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.starmap(analyze_stock, [(f, names_dict) for f in files])
    
    hits = [r for r in results if r is not None]
    
    if hits:
        result_df = pd.DataFrame(hits)
        
        # 排序：日期倒序(看最新信号) + 涨幅降序(看最强信号)
        result_df = result_df.sort_values(by=['日期', '涨跌幅_数值'], ascending=[False, False])
        result_df = result_df.drop(columns=['涨跌幅_数值'])
        
        now = datetime.now()
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        file_path = f"{dir_name}/pick_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        
        result_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ 扫描完成！共找到 {len(hits)} 个信号（已剔除ST/退市）")
        print(f"📅 优先展示最新交易日 ({result_df.iloc[0]['日期']}) 的最强个股：")
        print("-" * 65)
        print(result_df.head(15).to_string(index=False))
        print("-" * 65)
    else:
        print("\n当前市场环境下未发现符合条件的个股。")

if __name__ == "__main__":
    main()
