#!/usr/bin/env python3
"""
CS2数据库查看工具
直接查看并打印CS2数据库的投资组合和分析信号
必须指定实验名称(exp_name)来查看数据

使用方法：
  python view.py EXP_NAME                    # 查看指定实验的所有信息
  python view.py EXP_NAME portfolios         # 查看投资组合
  python view.py EXP_NAME positions           # 查看最新持仓
  python view.py EXP_NAME summary            # 查看数据摘要
  python view.py list                        # 列出所有实验
"""

import sqlite3
import json
from datetime import datetime
import sys
import os
from database.cs2_sqlite_setup import CS2_DB_PATH

DB_PATH = CS2_DB_PATH

def print_header(text):
    """打印标题"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")

def get_config_id_by_exp_name(cursor, exp_name):
    """通过exp_name获取config_id"""
    cursor.execute("SELECT id FROM cs2_config WHERE exp_name = ?", (exp_name,))
    row = cursor.fetchone()
    return row[0] if row else None

def list_experiments():
    """列出所有实验（从所有可用的数据库文件中，排除测试/临时实验）"""
    
    # 从当前配置的数据库收集实验
    all_experiments = []
    
    if not DB_PATH or not os.path.exists(DB_PATH):
        print_header("可用的实验列表")
        print("数据库文件不存在")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cs2_config'")
        if cursor.fetchone():
            cursor.execute("SELECT exp_name, updated_at, llm_provider, llm_model, has_planner FROM cs2_config ORDER BY updated_at DESC;")
            all_experiments = cursor.fetchall()
        
        conn.close()
    except Exception as e:
        print_header("可用的实验列表")
        print(f"无法读取数据库: {e}")
        return
    
    print_header("可用的实验列表")
    
    if not all_experiments:
        print("暂无实验数据")
        return
    
    # 按更新时间排序
    all_experiments.sort(key=lambda x: x[1] if x[1] else '', reverse=True)
    
    print(f"{'实验名称':<30} {'创建时间':<20} {'LLM':<20} {'Planner':<10}")
    print("-" * 85)
    
    for row in all_experiments:
        exp_name, updated_at, provider, model, has_planner = row
        planner_str = "启用" if has_planner else "禁用"
        llm_info = f"{provider}/{model}"
        date_str = updated_at[:19] if updated_at else "N/A"
        print(f"{exp_name:<30} {date_str:<20} {llm_info:<20} {planner_str:<10}")
    
    print("-" * 85)
    print(f"\n共 {len(all_experiments)} 个实验")

def view_portfolios(exp_name=None):
    """查看投资组合"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 构建查询SQL
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        if not config_id:
            print(f"❌ 实验 '{exp_name}' 不存在")
            conn.close()
            return
        sql = """
            SELECT p.trading_date, p.cashflow, p.total_assets 
            FROM cs2_portfolio p 
            WHERE p.config_id = ? 
            ORDER BY p.trading_date;
        """
        cursor.execute(sql, (config_id,))
        title = f"CS2 投资组合概览 - {exp_name}"
    else:
        sql = "SELECT trading_date, cashflow, total_assets FROM cs2_portfolio ORDER BY trading_date;"
        cursor.execute(sql)
        title = "CS2 投资组合概览 (所有实验)"
    
    rows = cursor.fetchall()
    
    print_header(title)
    
    if not rows:
        print("暂无数据")
        conn.close()
        return
    
    print(f"{'日期':<15} {'现金':<15} {'总资产':<15} {'收益率':<15}")
    print("-" * 80)
    
    # Get initial cashflow from the first portfolio record (from config's cashflow)
    initial = float(rows[0][2])  # Use total_assets of first record as initial
    prev_assets = initial
    
    for row in rows:
        date = row[0][:10]
        cash = row[1]
        assets = row[2]
        change = assets - prev_assets
        pct = (assets - initial) / initial * 100
        
        print(f"{date:<15} ${cash:<14.2f} ${assets:<14.2f} {pct:>+14.2f}%")
        prev_assets = assets
    
    print("-" * 80)
    
    if rows:
        latest_date = rows[-1][0][:10]
        latest_assets = rows[-1][2]
        total_pct = (latest_assets - initial) / initial * 100
        print(f"\n交易日：{len(rows)}天 | 起始：{rows[0][0][:10]} | 最新：{latest_date}")
        print(f"初始资产：${initial:.2f} | 最新资产：${latest_assets:.2f} | 收益率：{total_pct:.2f}%")
    
    conn.close()

def view_latest_positions(exp_name=None):
    """查看最新持仓"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 构建查询SQL
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        if not config_id:
            print(f"❌ 实验 '{exp_name}' 不存在")
            conn.close()
            return
        sql = """
            SELECT p.trading_date, p.positions 
            FROM cs2_portfolio p 
            WHERE p.config_id = ? 
            ORDER BY p.trading_date DESC 
            LIMIT 1;
        """
        cursor.execute(sql, (config_id,))
    else:
        sql = "SELECT trading_date, positions FROM cs2_portfolio ORDER BY trading_date DESC LIMIT 1;"
        cursor.execute(sql)
    
    result = cursor.fetchone()
    
    if not result:
        print("暂无数据")
        conn.close()
        return
    
    date, positions_json = result
    positions = json.loads(positions_json)
    
    exp_label = f" - {exp_name}" if exp_name else ""
    print_header(f"最新持仓明细 ({date[:10]}){exp_label}")
    
    print(f"{'物品名称':<50} {'持仓':<10} {'价值':<15} {'单价':<15}")
    print("-" * 95)
    
    total_value = 0
    active_count = 0
    
    # 按价值排序
    sorted_positions = sorted(
        [(item, data) for item, data in positions.items() if data['shares'] > 0],
        key=lambda x: x[1]['value'],
        reverse=True
    )
    
    for item, data in sorted_positions:
        price = data['value'] / data['shares'] if data['shares'] > 0 else 0
        print(f"{item[:49]:<50} {data['shares']:<10} ${data['value']:<14.2f} ${price:<14.2f}")
        total_value += data['value']
        active_count += 1
    
    print("-" * 95)
    print(f"活跃持仓数量：{active_count} | 总持仓价值：${total_value:.2f}")
    
    # 获取现金
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        cursor.execute("SELECT cashflow, total_assets FROM cs2_portfolio WHERE config_id = ? AND trading_date = ?;", (config_id, date))
    else:
        cursor.execute("SELECT cashflow, total_assets FROM cs2_portfolio WHERE trading_date = ?;", (date,))
    
    result = cursor.fetchone()
    if result:
        cash, total = result
        print(f"现金：${cash:.2f} | 总资产：${total:.2f}")
    
    conn.close()

def view_summary(exp_name=None):
    """查看数据摘要"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        if not config_id:
            print(f"❌ 实验 '{exp_name}' 不存在")
            conn.close()
            return
        title = f"CS2 数据库摘要 - {exp_name}"
        
        # 按config_id过滤
        cursor.execute("SELECT COUNT(*) FROM cs2_portfolio WHERE config_id = ?;", (config_id,))
        portfolio_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM cs2_decision d
            JOIN cs2_portfolio p ON d.portfolio_id = p.id
            WHERE p.config_id = ?;
        """, (config_id,))
        decision_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM cs2_signal s
            JOIN cs2_portfolio p ON s.portfolio_id = p.id
            WHERE p.config_id = ?;
        """, (config_id,))
        signal_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(trading_date), MAX(trading_date) FROM cs2_portfolio WHERE config_id = ?;", (config_id,))
        min_date, max_date = cursor.fetchone()
    else:
        title = "CS2 数据库摘要 (所有实验)"
        
        cursor.execute("SELECT COUNT(*) FROM cs2_portfolio;")
        portfolio_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM cs2_decision;")
        decision_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM cs2_signal;")
        signal_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(trading_date), MAX(trading_date) FROM cs2_portfolio;")
        min_date, max_date = cursor.fetchone()
    
    print_header(title)
    
    print(f"投资组合记录：{portfolio_count} 条")
    print(f"交易决策记录：{decision_count} 条")
    print(f"分析信号记录：{signal_count} 条")
    
    if min_date and max_date:
        print(f"日期范围：{min_date[:10]} 至 {max_date[:10]}")
    
    conn.close()

def main():
    """主函数"""
    # 解析参数
    exp_name = None
    args = []
    
    # 查找 --exp 参数
    i = 0
    while i < len(sys.argv):
        if sys.argv[i] == '--exp' and i + 1 < len(sys.argv):
            exp_name = sys.argv[i + 1]
            i += 2
        else:
            args.append(sys.argv[i])
            i += 1
    
    # 如果第一个参数不是命令，可能是实验名称
    if len(args) > 1 and args[1] not in ['portfolios', 'positions', 'summary', 'list', 'experiments']:
        if not exp_name:  # 如果还没有通过 --exp 指定，则作为实验名称
            exp_name = args[1]
            args = [args[0]] + args[2:]  # 移除实验名称，保留后续参数
    
    # 处理 list 命令（不需要实验名称）
    if len(args) > 1 and (args[1] == 'list' or args[1] == 'experiments'):
        list_experiments()
        return
    
    # 如果没有指定实验名称，先列出所有实验并提示
    if not exp_name:
        print("no exp name specified")
        list_experiments()
        return
    
    # 验证实验名称是否存在
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    config_id = get_config_id_by_exp_name(cursor, exp_name)
    conn.close()
    
    if not config_id:
        print(f"❌ 实验 '{exp_name}' 不存在\n")
        list_experiments()
        return
    
    # 执行相应的命令
    if len(args) == 1:
        # 显示摘要和最新持仓
        view_summary(exp_name)
        view_portfolios(exp_name)
        view_latest_positions(exp_name)
    elif args[1] == 'portfolios':
        view_portfolios(exp_name)
    elif args[1] == 'positions':
        view_latest_positions(exp_name)
    elif args[1] == 'summary':
        view_summary(exp_name)
    else:
        print("用法：")
        print("  python view.py EXP_NAME                      # 查看指定实验的所有信息")
        print("  python view.py EXP_NAME portfolios          # 查看投资组合")
        print("  python view.py EXP_NAME positions           # 查看最新持仓")
        print("  python view.py EXP_NAME summary             # 查看数据摘要")
        print("  python view.py list                         # 列出所有实验")
        print("\n或者使用 --exp 参数：")
        print("  python view.py --exp EXP_NAME portfolios")


if __name__ == "__main__":
    main()
