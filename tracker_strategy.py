import pandas as pd
import os
from datetime import datetime

def run_tracker():
    # 1. 自动寻找最新的选股结果文件
    today_month = datetime.now().strftime('%Y-%m')
    if not os.path.exists(today_month):
        print("尚未发现本月选股目录")
        return

    # 找到最新的 pick 文件
    pick_files = [f for f in os.listdir(today_month) if f.startswith('pick_')]
    if not pick_files:
        print("没有可追踪的标的")
        return
    
    latest_pick = sorted(pick_files)[-1]
    pick_df = pd.read_csv(os.path.join(today_month, latest_pick), dtype={'代码': str})
    watch_list = pick_df['代码'].tolist()

    tracker_results = []
    stock_data_dir = './stock_data'

    # 2. 仅对选出的标的进行深度买点扫描
    for code in watch_list:
        file_path = os.path.join(stock_data_dir, f"{code}.csv")
        if not os.path.exists(file_path): continue

        df = pd.read_csv(file_path, encoding='utf-8')
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        
        # 计算指标
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA13'] = df['收盘'].rolling(13).mean()
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 买点核心逻辑判断
        # 条件1：价格回踩13日线（差距在2%以内）
        dist_to_ma13 = abs(curr['收盘'] - curr['MA13']) / curr['MA13']
        is_near_ma13 = dist_to_ma13 <= 0.02
        
        # 条件2：极致缩量（当前成交量 < 近5日最大量的 40%）
        vol_5d_max = df['成交量'].tail(5).max()
        is_extreme_low_vol = curr['成交量'] <= vol_5d_max * 0.4
        
        # 条件3：止跌信号（今天收盘价不低于昨天最低价，或收阳线）
        is_stop_falling = curr['收盘'] >= prev['最低'] or curr['收盘'] > curr['开盘']

        if is_near_ma13 and is_extreme_low_vol and is_stop_falling:
            tracker_results.append({
                "代码": code,
                "名称": pick_df[pick_df['代码'] == code]['名称'].values[0],
                "状态": "回踩13日线买点确认",
                "距离13日线%": round(dist_to_ma13 * 100, 2),
                "收盘价": curr['收盘']
            })

    # 3. 输出追踪报告
    if tracker_results:
        res_df = pd.DataFrame(tracker_results)
        dir_name = "Buy_Signals"
        os.makedirs(dir_name, exist_ok=True)
        save_path = f"{dir_name}/signal_{datetime.now().strftime('%Y%m%d')}.csv"
        res_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"!!! 发现 {len(tracker_results)} 个买点信号，请检查 {save_path}")
    else:
        print("今日标的仍在调整中，暂无精准买点。")

if __name__ == "__main__":
    run_tracker()
