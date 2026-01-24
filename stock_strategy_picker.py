import pandas as pd
import os
from datetime import datetime
import multiprocessing

# 技术参数定义
MA_FAST = 13
MA_SLOW = 55
VMA_WINDOW = 5        # 成交量均线周期
LIMIT_UP_THRESHOLD = 9.8  # 涨停阈值
LOOKBACK_WINDOW = 6       # 检查最近6天

def analyze_stock(file_path, names_dict):
    try:
        # 读取CSV
        df = pd.read_csv(file_path, encoding='utf-8')
        if len(df) < MA_SLOW + VMA_WINDOW:
            return None
        
        # 确保按时间升序
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        
        # --- 计算技术指标 ---
        df['MA13'] = df['收盘'].rolling(window=MA_FAST).mean()
        df['MA55'] = df['收盘'].rolling(window=MA_SLOW).mean()
        df['VMA5'] = df['成交量'].rolling(window=VMA_WINDOW).mean()
        
        # 获取最近的数据切片
        recent_df = df.tail(LOOKBACK_WINDOW)
        current_day = recent_df.iloc[-1]
        previous_days = recent_df.iloc[:-1]
        yesterday = previous_days.iloc[-1]
        
        # --- 筛选逻辑 ---
        # 1. 前几日有过首板
        has_recent_limit_up = any(previous_days['涨跌幅'] >= LIMIT_UP_THRESHOLD)
        
        # 2. 趋势支撑：MA13 > MA55 且当前价在MA13附近
        on_support = current_day['MA13'] > current_day['MA55'] and \
                     current_day['收盘'] >= current_day['MA13'] * 0.99
        
        # 3. 前期缩量：昨日成交量小于前几日最大成交量的 70%
        max_vol_recent = previous_days['成交量'].max()
        is_shrinking_vol = yesterday['成交量'] < max_vol_recent * 0.7
        
        # 4. 买点确认：今日放量阳线 (涨幅>1%, 且放量超过VMA5)
        is_positive_candle = current_day['涨跌幅'] > 1.0
        is_volume_rebound = current_day['成交量'] > yesterday['成交量'] and \
                            current_day['成交量'] > current_day['VMA5']
        
        if has_recent_limit_up and on_support and is_shrinking_vol and is_positive_candle and is_volume_rebound:
            raw_code = str(current_day['股票代码']).split('.')[0]
            code = raw_code.zfill(6)
            name = names_dict.get(code, "未知名称")
            
            return {
                "代码": code,
                "名称": name,
                "日期": current_day['日期'].strftime('%Y-%m-%d'),
                "收盘价": current_day['收盘'],
                "涨跌幅_数值": current_day['涨跌幅'], # 用于排序的数值
                "涨跌幅": f"{current_day['涨跌幅']}%",
                "成交量比VMA5": round(current_day['成交量'] / current_day['VMA5'], 2),
                "换手率": current_day['换手率']
            }
    except Exception:
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
    print(f"正在扫描 {len(files)} 只个股...")
    
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.starmap(analyze_stock, [(f, names_dict) for f in files])
    
    hits = [r for r in results if r is not None]
    
    if hits:
        result_df = pd.DataFrame(hits)
        
        # --- 新增排序功能 ---
        # 优先按“涨跌幅”降序排列，如果涨跌幅相同，按“成交量比VMA5”降序排列
        result_df = result_df.sort_values(by=['涨跌幅_数值', '成交量比VMA5'], ascending=False)
        
        # 删除排序用的临时列
        result_df = result_df.drop(columns=['涨跌幅_数值'])
        
        now = datetime.now()
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        
        file_path = f"{dir_name}/pick_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        result_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f"\n✨ 发现信号！已按强度排序（涨幅从高到低）")
        print(f"结果已存入: {file_path}")
        # 控制台打印前 15 名
        print(result_df.head(15).to_string(index=False))
    else:
        print("\n当前未发现符合‘回踩放量确认’条件的个股。")

if __name__ == "__main__":
    main()
