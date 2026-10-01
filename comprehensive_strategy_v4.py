import os
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

# ==============================================================================
# 升级目标：分文件夹存放 + 信号精简化
# ==============================================================================

DATA_DIR = 'stock_data'
NAMES_FILE = 'stock_names.csv'
OUTPUT_BASE = 'results'

COL_MAP = {
    '日期': 'date', '开盘': 'open', '最高': 'high', 
    '最低': 'low', '收盘': 'close', '成交量': 'volume', '涨跌幅': 'pct_chg'
}

def check_all_strategies(df):
    """检测所有战法逻辑，返回命中的【所有】战法列表"""
    if len(df) < 65: return []
    
    hits = []
    c, l, h, o, v, p = df['close'].values, df['low'].values, df['high'].values, df['open'].values, df['volume'].values, df['pct_chg'].values
    ma5, ma13, ma21, ma60 = [df['close'].rolling(w).mean().values for w in [5, 13, 21, 60]]
    v_ma5 = df['volume'].rolling(5).mean().values

    # --- 1. 隔山打牛 (核心爆发) ---
    for i in range(2, 15):
        idx = len(df) - i
        if p[idx-1] > 9.5 and c[idx] < o[idx] and v[idx] == max(v[idx-20:idx+1]):
            for j in range(idx + 1, len(df)):
                if c[j] < o[j] and v[j] < v[idx] * 0.5:
                    if c[-1] > h[j] and all(l[idx:] >= l[idx]):
                        hits.append("隔山打牛")
                        break

    # --- 2. 高量不破 (强力支撑) ---
    for i in range(1, 10):
        idx = len(df) - 1 - i
        if v[idx] > v_ma5[idx] * 2.5 and c[idx] > o[idx]: # 提高倍数至2.5，更严苛
            if all(l[idx+1:] >= l[idx]) and c[-1] > h[idx]:
                hits.append("高量不破")
                break

    # --- 3. 三位一体 (趋势转折) ---
    if c[-1] > ma60[-1] and c[-2] <= ma60[-2] and v[-1] > v_ma5[-1] * 1.5:
        hits.append("三位一体")

    # --- 4. 草上飞 (波段控盘 - 增加成交量过滤防止泛滥) ---
    if ma5[-1] > ma13[-1] > ma21[-1] and l[-1] >= ma13[-1]:
        if c[-1] > ma5[-1] and v[-1] > v_ma5[-1]: # 必须放量才算飞
            hits.append("草上飞")

    # --- 5. 追涨停 (空中加油) ---
    if len(df) >= 3 and p[-2] > 9.5 and v[-1] < v[-2] and c[-1] > o[-1]:
        if c[-1] > h[-2] * 0.98: # 靠近涨停高点准备突破
            hits.append("追涨停")

    return hits

def process_stock(file_name):
    code = file_name.split('.')[0]
    if code.startswith(('30', '68')): return None 
    try:
        df = pd.read_csv(os.path.join(DATA_DIR, file_name))
        df = df.rename(columns=COL_MAP)
        if df['pct_chg'].dtype == object:
            df['pct_chg'] = df['pct_chg'].str.replace('%', '').astype(float)
        
        hit_list = check_all_strategies(df)
        if hit_list:
            return {'code': code, 'strategies': hit_list, 'last_pct': df['pct_chg'].iloc[-1]}
    except: return None
    return None

def main():
    if not os.path.exists(NAMES_FILE): return
    names_df = pd.read_csv(NAMES_FILE)
    names_df['code'] = names_df['code'].astype(str).str.zfill(6)
    valid_codes = set(names_df[~names_df['name'].str.contains('ST|st')]['code'])

    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f.split('.')[0] in valid_codes]
    with ProcessPoolExecutor() as executor:
        results = [r for r in executor.map(process_stock, files) if r is not None]
    
    if results:
        now_str = datetime.now().strftime('%Y%m%d')
        # 按战法分类
        strategy_buckets = {}
        for r in results:
            for s in r['strategies']:
                if s not in strategy_buckets: strategy_buckets[s] = []
                # 合并名称
                name = names_df[names_df['code'] == r['code']]['name'].values[0]
                strategy_buckets[s].append({'代码': r['code'], '名称': name, '今日涨幅': r['last_pct']})

        # 分别输出
        for s_name, stocks in strategy_buckets.items():
            s_df = pd.DataFrame(stocks).sort_values(by='今日涨幅', ascending=False)
            out_dir = os.path.join(OUTPUT_BASE, now_str, s_name)
            os.makedirs(out_dir, exist_ok=True)
            s_df.to_csv(os.path.join(out_dir, f"{s_name}_结果.csv"), index=False, encoding='utf-8-sig')
            print(f"战法【{s_name}】保存成功：获取到 {len(stocks)} 只标的")

if __name__ == '__main__':
    main()
