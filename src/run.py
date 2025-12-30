#!/usr/bin/env python3
"""
批量运行CS2实验脚本
从指定开始日期到结束日期，依次运行实验
支持通过命令行参数指定实验配置
自动从配置文件目录发现可用的实验
"""

import sys
import os
import subprocess
import argparse
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# 获取项目根目录（脚本所在目录的父目录）
_script_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(_script_dir)


def run_experiment(trading_date: str, config_path: str, use_local_db: bool = True):
    """运行单个日期的实验"""
    
    src_dir = os.path.join(PROJECT_ROOT, "src")
    main_path = os.path.join(src_dir, "main.py")
    
    # Load .env and keep CS2_DB_PATH as relative path for subprocess
    load_dotenv()
    db_path = os.getenv("CS2_DB_PATH", "assets/cs2.db")
    
    env = os.environ.copy()
    env["CS2_DB_PATH"] = db_path
    
    cmd = [sys.executable, main_path, "--config", config_path, "--trading-date", trading_date]
    if use_local_db:
        cmd.append("--local-db")
    
    print(f"\n{'='*80}")
    print(f"开始运行: {trading_date}")
    print(f"{'='*80}\n")
    
    # Run subprocess from src directory to match main.py behavior
    result = subprocess.run(cmd, cwd=src_dir, env=env, capture_output=False, text=True)
    
    if result.returncode == 0:
        print(f"\n✅ {trading_date} 运行成功")
        return True
    else:
        print(f"\n❌ {trading_date} 运行失败 (退出码: {result.returncode})")
        return False


def main():
    """主函数：批量运行实验"""
    epilog = """
示例用法:
  # 使用配置文件文件名（自动在 src/config/ 目录查找）
  python run.py --config T-ds.yaml
  
  # 指定日期范围
  python run.py --config T-ds.yaml --start-date 2025-09-25 --end-date 2025-11-15
    """
    
    parser = argparse.ArgumentParser(
        description='批量运行CS2实验',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog
    )
    
    parser.add_argument('--config', type=str, required=True, help='配置文件文件名（自动在 src/config/ 目录查找，例如: T-ds.yaml)')
    parser.add_argument('--start-date', type=str, default='2025-09-25', help='开始日期 (格式: YYYY-MM-DD, 默认: 2025-09-25)')
    parser.add_argument('--end-date', type=str, default='2025-10-27', help='结束日期 (格式: YYYY-MM-DD, 默认: 2025-10-27)')
    parser.add_argument('--no-local-db', action='store_true', help='不使用本地数据库（默认使用本地数据库）')
    
    args = parser.parse_args()
    
    # 确定配置文件路径（只支持文件名，自动在 src/config/ 目录查找）
    config_path = args.config
    
    # 只支持文件名（不能包含路径）
    if os.path.dirname(config_path):
        print(f"❌ 错误: 配置文件参数只能使用文件名，不能包含路径")
        print(f"   正确用法: python run.py --config T-ds.yaml")
        sys.exit(1)
    
    # 尝试从配置文件读取实验名称
    try:
        # 自动在 src/config/ 目录查找
        abs_config_path = os.path.join(PROJECT_ROOT, "src", "config", config_path)
            
        with open(abs_config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            exp_name = config.get('exp_name')
        # Update config_path to be relative to src directory (for subprocess cwd=src)
        config_path = os.path.join("config", config_path)
    except FileNotFoundError as e:
        print(f"❌ 错误: 找不到配置文件 '{config_path}'")
        sys.exit(1)
    except Exception as e:
        print(f"❌ 错误: 无法读取配置文件 '{config_path}': {e}")
        sys.exit(1)
    
    # 解析日期
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        print(f"❌ 错误: 日期格式不正确 - {e}")
        print("日期格式应为: YYYY-MM-DD (例如: 2025-09-25)")
        sys.exit(1)
    
    if start_date > end_date:
        print("❌ 错误: 开始日期不能晚于结束日期")
        sys.exit(1)
    
    use_local_db = not args.no_local_db
    
    # 确保日志目录存在
    Path(PROJECT_ROOT, "src", "logs").mkdir(parents=True, exist_ok=True)
    
    current_date = start_date
    
    print(f"\n{'='*80}")
    print(f"开始批量运行实验")
    print(f"配置文件: {config_path}")
    print(f"实验名称: {exp_name}")
    print(f"日期范围: {start_date.date()} 到 {end_date.date()}")
    print(f"总天数: {(end_date - start_date).days + 1}")
    print(f"使用本地数据库: {use_local_db}")
    print(f"{'='*80}\n")
    
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        success = run_experiment(date_str, config_path, use_local_db=use_local_db)
        
        if not success:
            print(f"\n❌ 日期 {date_str} 运行失败，程序退出")
            sys.exit(1)
        
        current_date += timedelta(days=1)
    
    # 输出总结
    print(f"\n{'='*80}")
    print(f"批量运行完成")
    print(f"成功运行 {((end_date - start_date).days + 1)} 个日期")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()

