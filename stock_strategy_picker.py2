import pandas as pd
import os
from datetime import datetime
import multiprocessing

# 技术参数定义
MA_FAST = 13
MA_SLOW = 55
LIMIT_UP_THRESHOLD = 9.8  # 涨停阈值
LOOKBACK_WINDOW = 6       # 检查最近6天（包含今天）

def analyze_stock(file_path, names_dict):
    try:
        # 读取CSV，指定编码以处理中文列名
        df = pd.read_csv(file_path, encoding='utf-8')
        if len(df) < MA_SLOW:
            return None
        
        # 确保按时间升序
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        
        # 计算技术指标
        df['MA13'] = df['收盘'].rolling(window=MA_FAST).mean()
        df['MA55'] = df['收盘'].rolling(window=MA_SLOW).mean()
        
        # 获取最近的数据切片
        recent_df = df.tail(LOOKBACK_WINDOW)
        current_day = recent_df.iloc[-1]
        previous_days = recent_df.iloc[:-1]
        
        # 筛选条件1：前几日有过首板（涨幅 > 9.8%）
        has_recent_limit_up = any(previous_days['涨跌幅'] >= LIMIT_UP_THRESHOLD)
        
        # 筛选条件2：回踩支撑（当前价在MA13上方，且MA13 > MA55）
        on_support = current_day['收盘'] >= current_day['MA13'] and current_day['MA13'] > current_day['MA55']
        
        # 筛选条件3：缩量回调（当前成交量小于前几日最大成交量的 70%）
        max_vol_recent = previous_days['成交量'].max()
        is_shrinking_vol = current_day['成交量'] < max_vol_recent * 0.7
        
        if has_recent_limit_up and on_support and is_shrinking_vol:
            # 格式化代码，确保是6位字符串
            raw_code = str(current_day['股票代码']).split('.')[0]
            code = raw_code.zfill(6)
            name = names_dict.get(code, "未知名称")
            
            return {
                "代码": code,
                "名称": name,
                "日期": current_day['日期'].strftime('%Y-%m-%d'),
                "收盘价": current_day['收盘'],
                "涨跌幅": current_day['涨跌幅'],
                "换手率": current_day['换手率']
            }
    except Exception as e:
        # print(f"Error processing {file_path}: {e}")
        return None
    return None

def main():
    stock_data_dir = './stock_data'
    
    # 匹配 stock_names.csv (字段为 code, name)
    try:
        names_df = pd.read_csv('stock_names.csv', dtype={'code': str})
        names_dict = dict(zip(names_df['code'].str.zfill(6), names_df['name']))
    except:
        names_dict = {}

    # 获取所有待扫描文件
    files = [os.path.join(stock_data_dir, f) for f in os.listdir(stock_data_dir) if f.endswith('.csv')]
    
    # 并行扫描提高速度
    with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
        results = pool.starmap(analyze_stock, [(f, names_dict) for f in files])
    
    # 汇总有效结果
    hits = [r for r in results if r is not None]
    
    if hits:
        result_df = pd.DataFrame(hits)
        now = datetime.now()
        # 创建年月目录 (例如: 2025-12)
        dir_name = now.strftime('%Y-%m')
        os.makedirs(dir_name, exist_ok=True)
        
        # 文件名带时间戳
        file_path = f"{dir_name}/pick_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        result_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"扫描完成，找到 {len(hits)} 个目标，已存入 {file_path}")
    else:
        print("未发现符合条件的个股")

if __name__ == "__main__":
    main()
