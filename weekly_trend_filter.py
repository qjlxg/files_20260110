import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# 配置参数
DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results'

def calculate_macd(df, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    df['dif'] = exp1 - exp2
    df['dea'] = df['dif'].ewm(span=signal, adjust=False).mean()
    df['macd'] = 2 * (df['dif'] - df['dea'])
    return df

def process_single_stock(file_name):
    """处理单只股票的筛选逻辑"""
    code = file_name.split('.')[0]
    
    # 基础代码过滤：必须是6位数字且排除30开头的创业板
    if not (code.isdigit() and len(code) == 6) or code.startswith('30'):
        return None
    
    try:
        file_path = os.path.join(DATA_DIR, file_name)
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        # 条件1：最新收盘价在 5.0 - 20.0 元之间
        last_close = df['close'].iloc[-1]
        if not (5.0 <= last_close <= 20.0):
            return None

        # 转换为周线数据
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        # 聚合：开盘价(首)、最高(最)、最低(最)、收盘(末)、成交量(和)
        df_weekly = df.resample('W').agg({
            'open':'first', 
            'high':'max', 
            'low':'min', 
            'close':'last', 
            'volume':'sum'
        }).dropna()
        
        # 计算周线MACD
        df_weekly = calculate_macd(df_weekly)
        
        if len(df_weekly) < 5: return None
        
        last_week = df_weekly.iloc[-1]
        prev_weeks_vol = df_weekly['volume'].iloc[-5:-1].mean()
        
        # 图片逻辑实现：
        # 1. MACD在水上（DIF和DEA均大于0）- 代表中长线走强
        is_water_up = last_week['dif'] > 0 and last_week['dea'] > 0
        # 2. 当前处于金叉或多头排列（DIF > DEA）- 代表正处于主升阶段
        is_gold_cross = last_week['dif'] > last_week['dea']
        # 3. 成交量配合（当前周量比过去4周均量放大1.2倍以上）- 对应图中"3"的放量
        vol_breakout = last_week['volume'] > prev_weeks_vol * 1.2
        
        if is_water_up and is_gold_cross and vol_breakout:
            return code
    except Exception:
        return None
    return None

def main():
    # 读取股票名称映射表并排除ST
    if not os.path.exists(NAMES_FILE):
        print(f"错误: 找不到 {NAMES_FILE}")
        return
        
    names_df = pd.read_csv(NAMES_FILE)
    names_df['code'] = names_df['code'].astype(str).str.zfill(6)
    # 排除名称中带ST的
    names_df = names_df[~names_df['name'].str.contains('ST|st')]
    valid_codes_set = set(names_df['code'])

    # 扫描数据目录
    if not os.path.exists(DATA_DIR):
        print(f"错误: 目录 {DATA_DIR} 不存在")
        return
        
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f.split('.')[0] in valid_codes_set]
    
    # 并行处理以提高速度
    print(f"开始扫描 {len(files)} 个文件...")
    with ProcessPoolExecutor() as executor:
        filtered_codes = list(executor.map(process_single_stock, files))
    
    results = [c for c in filtered_codes if c is not None]
    
    # 关联名称并保存
    final_df = names_df[names_df['code'].isin(results)]
    
    # 创建年月目录 (例如: results/202512/)
    now = datetime.now()
    dir_path = os.path.join(OUTPUT_BASE, now.strftime('%Y%m'))
    os.makedirs(dir_path, exist_ok=True)
    
    # 保存结果，文件名带时间戳
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(dir_path, f'pick_{timestamp}.csv')
    
    final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"任务完成！筛选出 {len(final_df)} 只符合周线主升浪形态的股票。")
    print(f"结果路径: {file_path}")

if __name__ == '__main__':
    main()
