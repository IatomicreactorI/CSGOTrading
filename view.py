"""
CS2 database view tool
View and print CS2 database portfolios and analyst signals
Must specify experiment name (exp_name) to view data

Usage:
  python view.py EXP_NAME                    # View all information of specified experiment
  python view.py EXP_NAME portfolios         # View portfolios
  python view.py EXP_NAME positions           # View latest positions
  python view.py EXP_NAME daily              # View daily portfolios and export CSV
  python view.py EXP_NAME daily DATE         # View portfolios of specified date
  python view.py EXP_NAME thinking           # Export thinking process JSON file
  python view.py EXP_NAME summary            # View data summary
  python view.py list                        # List all experiments
"""

import sqlite3
import json
from datetime import datetime
import sys
import os
import csv
from database.cs2_sqlite_setup import CS2_DB_PATH

DB_PATH = CS2_DB_PATH

def print_header(text):
    """Print header"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")

def get_config_id_by_exp_name(cursor, exp_name):
    """Get config_id by exp_name"""
    cursor.execute("SELECT id FROM cs2_config WHERE exp_name = ?", (exp_name,))
    row = cursor.fetchone()
    return row[0] if row else None

def list_experiments():
    """List all experiments (from all available database files, excluding test/temporary experiments)"""
    
    # Collect experiments from current configured database
    all_experiments = []
    
    if not DB_PATH or not os.path.exists(DB_PATH):
        print_header("Available experiments list")
        print("Database file does not exist")
        return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cs2_config'")
        if cursor.fetchone():
            cursor.execute("SELECT exp_name, updated_at, llm_provider, llm_model, has_planner FROM cs2_config ORDER BY updated_at DESC;")
            all_experiments = cursor.fetchall()
        
        conn.close()
    except Exception as e:
        print_header("Available experiments list")
        print(f"Failed to read database: {e}")
        return
    
    print_header("Available experiments list")
    
    if not all_experiments:
        print("No experiment data")
        return
    
    # Sort by update time
    all_experiments.sort(key=lambda x: x[1] if x[1] else '', reverse=True)
    
    print(f"{'Experiment name':<30} {'Created time':<20} {'LLM':<20} {'Planner':<10}")
    print("-" * 85)
    
    for row in all_experiments:
        exp_name, updated_at, provider, model, has_planner = row
        planner_str = "Enabled" if has_planner else "Disabled"
        llm_info = f"{provider}/{model}"
        date_str = updated_at[:19] if updated_at else "N/A"
        print(f"{exp_name:<30} {date_str:<20} {llm_info:<20} {planner_str:<10}")
    
    print("-" * 85)
    print(f"\n{len(all_experiments)} experiments in total")

def view_portfolios(exp_name=None):
    """View portfolios"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Build query SQL
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        if not config_id:
            print(f"Experiment '{exp_name}' does not exist")
            conn.close()
            return
        sql = """
            SELECT p.trading_date, p.cashflow, p.total_assets 
            FROM cs2_portfolio p 
            WHERE p.config_id = ? 
            ORDER BY p.trading_date;
        """
        cursor.execute(sql, (config_id,))
        title = f"CS2 portfolios overview - {exp_name}"
    else:
        sql = "SELECT trading_date, cashflow, total_assets FROM cs2_portfolio ORDER BY trading_date;"
        cursor.execute(sql)
        title = "CS2 portfolios overview (all experiments)"
    
    rows = cursor.fetchall()
    
    print_header(title)
    
    if not rows:
        print("No data")
        conn.close()
        return
    
    print(f"{'Date':<15} {'Cash':<15} {'Total assets':<15} {'Return':<15}")
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
        print(f"\nTrading days: {len(rows)} | Start: {rows[0][0][:10]} | Latest: {latest_date}")
        print(f"Initial assets: ${initial:.2f} | Latest assets: ${latest_assets:.2f} | Return: {total_pct:.2f}%")
    
    conn.close()

def view_latest_positions(exp_name=None):
    """View latest positions"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Build query SQL
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        if not config_id:
            print(f"Experiment '{exp_name}' does not exist")
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
        print("No data")
        conn.close()
        return
    
    date, positions_json = result
    positions = json.loads(positions_json)
    
    exp_label = f" - {exp_name}" if exp_name else ""
    print_header(f"Latest positions detail ({date[:10]}){exp_label}")
    
    print(f"{'Item name':<50} {'Shares':<10} {'Value':<15} {'Price':<15}")
    print("-" * 95)
    
    total_value = 0
    active_count = 0
    
    # Sort by value
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
    print(f"Active holdings: {active_count} | Total positions value: ${total_value:.2f}")
    
    # Get cash
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        cursor.execute("SELECT cashflow, total_assets FROM cs2_portfolio WHERE config_id = ? AND trading_date = ?;", (config_id, date))
    else:
        cursor.execute("SELECT cashflow, total_assets FROM cs2_portfolio WHERE trading_date = ?;", (date,))
    
    result = cursor.fetchone()
    if result:
        cash, total = result
        print(f"Cash: ${cash:.2f} | Total assets: ${total:.2f}")
    
    conn.close()

def export_portfolio_to_csv(exp_name, rows):
    """Export portfolios data to CSV file"""
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), 'view_portfolio')
    os.makedirs(output_dir, exist_ok=True)
    
    # CSV file path
    csv_filename = os.path.join(output_dir, f"{exp_name}.csv")
    
    # Collect all dates and all items
    dates_set = set()
    all_items = set()
    date_positions = {}  # {date: {item: shares}}
    date_total_values = {}  # {date: total_value}
    date_total_assets = {}  # {date: total_assets}
    
    for row in rows:
        date, cash, total_assets, positions_json = row
        date_str = date[:10]  # YYYY-MM-DD
        dates_set.add(date_str)
        
        positions = json.loads(positions_json) if positions_json else {}
        date_positions[date_str] = {}
        total_value = 0
        
        for item, data in positions.items():
            shares = data.get('shares', 0)
            value = data.get('value', 0)
            if shares > 0:  # Only record items with holdings
                all_items.add(item)
                date_positions[date_str][item] = shares
                total_value += value
        
        date_total_values[date_str] = total_value
        date_total_assets[date_str] = total_assets
    
    # Sort: date from small to large, item in alphabetical order
    dates = sorted(dates_set)
    sorted_items = sorted(all_items)
    
    # Write to CSV
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # First row: date title
            header = ['Item name'] + dates
            writer.writerow(header)
            
            # Each row: item name + the number of shares of the item in each date
            for item in sorted_items:
                row = [item]
                for date in dates:
                    shares = date_positions.get(date, {}).get(item, 0)
                    row.append(shares)
                writer.writerow(row)
            
            # Total positions value row
            total_row = ['Total positions value']
            for date in dates:
                total_row.append(f"${date_total_values.get(date, 0):.2f}")
            writer.writerow(total_row)
            
            # Total assets value row
            total_assets_row = ['Total assets value']
            for date in dates:
                total_assets_row.append(f"${date_total_assets.get(date, 0):.2f}")
            writer.writerow(total_assets_row)
        
        print(f"\nCSV file exported: {csv_filename}")
    except Exception as e:
        print(f"\nFailed to export CSV file: {e}")

def export_thinking_process(exp_name, rows):
    """Export thinking process (decisions and analyst signals) to JSON file"""
    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), 'view_thinking')
    os.makedirs(output_dir, exist_ok=True)
    
    # JSON file path
    json_filename = os.path.join(output_dir, f"{exp_name}.json")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get config_id
    config_id = get_config_id_by_exp_name(cursor, exp_name)
    if not config_id:
        print(f"\nExperiment '{exp_name}' does not exist")
        conn.close()
        return
    
    # Organize thinking process data by date
    thinking_data = {}
    
    # Iterate through each date, query the decisions and signals of the date
    for row in rows:
        date, cash, total_assets, positions_json = row
        date_str = date[:10]
        
        # Get the portfolio_id of the date
        cursor.execute("""
            SELECT id FROM cs2_portfolio 
            WHERE config_id = ? AND DATE(trading_date) = DATE(?)
            ORDER BY updated_at DESC
            LIMIT 1
        """, (config_id, date))
        portfolio_result = cursor.fetchone()
        
        if not portfolio_result:
            continue
        
        portfolio_id = portfolio_result[0]
        
        # Initialize the data structure of the date
        if date_str not in thinking_data:
            thinking_data[date_str] = {
                'decisions': [],
                'signals_by_item': {}
            }
        
        # Query all decisions of the date
        cursor.execute("""
            SELECT d.item_name, d.action, d.quantity, d.price, 
                   d.justification, d.llm_prompt
            FROM cs2_decision d
            WHERE d.portfolio_id = ?
            ORDER BY d.item_name
        """, (portfolio_id,))
        
        decisions = cursor.fetchall()
        
        for decision in decisions:
            decision_dict = {
                'item_name': decision[0],
                'action': decision[1],
                'quantity': decision[2],
                'price': float(decision[3]),
                'justification': decision[4],
                'llm_prompt': decision[5]
            }
            thinking_data[date_str]['decisions'].append(decision_dict)
        
        # Query all signals of the date
        cursor.execute("""
            SELECT s.item_name, s.analyst, s.signal, 
                   s.justification, s.llm_prompt
            FROM cs2_signal s
            WHERE s.portfolio_id = ?
            ORDER BY s.item_name, s.analyst
        """, (portfolio_id,))
        
        signals = cursor.fetchall()
        
        for signal in signals:
            item_name = signal[0]
            analyst = signal[1]
            signal_dict = {
                'analyst': analyst,
                'signal': signal[2],
                'justification': signal[3],
                'llm_prompt': signal[4]
            }
            
            if item_name not in thinking_data[date_str]['signals_by_item']:
                thinking_data[date_str]['signals_by_item'][item_name] = []
            
            thinking_data[date_str]['signals_by_item'][item_name].append(signal_dict)
    
    conn.close()
    
    # Write to JSON file
    try:
        with open(json_filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(thinking_data, jsonfile, ensure_ascii=False, indent=2)
        
        print(f"Thinking process exported: {json_filename}")
    except Exception as e:
        print(f"Failed to export Thinking process: {e}")

def view_daily_positions(exp_name=None, target_date=None):
    """View daily portfolios detail"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Build query SQL
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        if not config_id:
            print(f"Experiment '{exp_name}' does not exist")
            conn.close()
            return
        
        if target_date:
            # View specified date
            sql = """
                SELECT p.trading_date, p.cashflow, p.total_assets, p.positions 
                FROM cs2_portfolio p 
                WHERE p.config_id = ? AND DATE(p.trading_date) = DATE(?)
                ORDER BY p.trading_date;
            """
            cursor.execute(sql, (config_id, target_date))
            title = f"Portfolios detail - {exp_name} ({target_date})"
        else:
            # View all dates
            sql = """
                SELECT p.trading_date, p.cashflow, p.total_assets, p.positions 
                FROM cs2_portfolio p 
                WHERE p.config_id = ? 
                ORDER BY p.trading_date;
            """
            cursor.execute(sql, (config_id,))
            title = f"Daily portfolios detail - {exp_name}"
    else:
        if target_date:
            sql = """
                SELECT trading_date, cashflow, total_assets, positions 
                FROM cs2_portfolio 
                WHERE DATE(trading_date) = DATE(?)
                ORDER BY trading_date;
            """
            cursor.execute(sql, (target_date,))
            title = f"Portfolio Details (All Experiments) - {target_date}"
        else:
            sql = "SELECT trading_date, cashflow, total_assets, positions FROM cs2_portfolio ORDER BY trading_date;"
            cursor.execute(sql)
            title = "Daily portfolios detail (All experiments)"
    
    rows = cursor.fetchall()
    
    if not rows:
        print("No data")
        conn.close()
        return
    
    #if only query one day, directly display
    if target_date or len(rows) == 1:
        for row in rows:
            date, cash, total_assets, positions_json = row
            positions = json.loads(positions_json) if positions_json else {}
            
            print_header(f"{title} - {date[:10]}")
            
            print(f"{'Date':<15} {'Cash':<15} {'Total assets':<15}")
            print("-" * 50)
            print(f"{date[:10]:<15} ${cash:<14.2f} ${total_assets:<14.2f}")
            print("-" * 50)
            print()
            
            print(f"{'Item name':<50} {'Holdings':<10} {'Value':<15} {'Price':<15}")
            print("-" * 95)
            
            total_value = 0
            active_count = 0
            
            sorted_positions = sorted(
                [(item, data) for item, data in positions.items() if data.get('shares', 0) > 0],
                key=lambda x: x[1].get('value', 0),
                reverse=True
            )
            
            for item, data in sorted_positions:
                shares = data.get('shares', 0)
                value = data.get('value', 0)
                price = value / shares if shares > 0 else 0
                print(f"{item[:49]:<50} {shares:<10} ${value:<14.2f} ${price:<14.2f}")
                total_value += value
                active_count += 1
            
            print("-" * 95)
            print(f"Actice holding: {active_count} | Total assets Value: ${total_value:.2f}")
            print(f"Cash: ${cash:.2f} | Total assets${total_assets:.2f}")
            print()
    else:
        # Display all the date
        print_header(title)
        
        initial = float(rows[0][2])  # Use total_assets of first record as initial
        
        for row in rows:
            date, cash, total_assets, positions_json = row
            positions = json.loads(positions_json) if positions_json else {}
            
            # Calculate active holdings
            active_positions = [(item, data) for item, data in positions.items() 
                              if data.get('shares', 0) > 0]
            active_count = len(active_positions)
            total_value = sum(data.get('value', 0) for _, data in active_positions)
            pct = (total_assets - initial) / initial * 100
            
            print(f"\n{'='*95}")
            print(f"Date: {date[:10]} | Cash: ${cash:.2f} | Total assets: ${total_assets:.2f} | Return: {pct:+.2f}%")
            print(f"{'='*95}")
            
            if active_count > 0:
                print(f"{'Item name':<50} {'Holding':<10} {'Value':<15} {'Price':<15}")
                print("-" * 95)
                
                sorted_positions = sorted(
                    active_positions,
                    key=lambda x: x[1].get('value', 0),
                    reverse=True
                )
                
                for item, data in sorted_positions:
                    shares = data.get('shares', 0)
                    value = data.get('value', 0)
                    price = value / shares if shares > 0 else 0
                    print(f"{item[:49]:<50} {shares:<10} ${value:<14.2f} ${price:<14.2f}")
                
                print("-" * 95)
                print(f"Active holding amount: {active_count} | Total asset value: ${total_value:.2f}")
            else:
                print("No active position")
        
        print(f"\n{'='*95}")
        latest_date = rows[-1][0][:10]
        latest_assets = rows[-1][2]
        total_pct = (latest_assets - initial) / initial * 100
        print(f"Trading days:{len(rows)}| Starting from{rows[0][0][:10]} | Latest data:{latest_date}")
        print(f"Inititial assets:${initial:.2f} | Latest assets:${latest_assets:.2f} | Return:{total_pct:.2f}%")
        
        # Export the CSV only when all dates are viewed and there is an experiment name
        if exp_name and not target_date:
            export_portfolio_to_csv(exp_name, rows)
    
    conn.close()

def get_portfolio_rows(exp_name):
    """Get the portfolio data rows for export"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        if not config_id:
            print(f"Experiment '{exp_name}' does not exist")
            conn.close()
            return None
        
        sql = """
            SELECT p.trading_date, p.cashflow, p.total_assets, p.positions 
            FROM cs2_portfolio p 
            WHERE p.config_id = ? 
            ORDER BY p.trading_date;
        """
        cursor.execute(sql, (config_id,))
    else:
        sql = "SELECT trading_date, cashflow, total_assets, positions FROM cs2_portfolio ORDER BY trading_date;"
        cursor.execute(sql)
    
    rows = cursor.fetchall()
    conn.close()
    return rows

def thinking_command(exp_name):
    """Export thinking process"""
    rows = get_portfolio_rows(exp_name)
    if rows:
        export_thinking_process(exp_name, rows)
    else:
        print("Can not get data")

def view_summary(exp_name=None):
    """View the data summary"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if exp_name:
        config_id = get_config_id_by_exp_name(cursor, exp_name)
        if not config_id:
            print(f"Experiment '{exp_name}' does not exist")
            conn.close()
            return
        title = f"CS2 database summary - {exp_name}"
        
        #fliter by config_id
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
        title = "CS2 database summary (all experiment)"
        
        cursor.execute("SELECT COUNT(*) FROM cs2_portfolio;")
        portfolio_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM cs2_decision;")
        decision_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM cs2_signal;")
        signal_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(trading_date), MAX(trading_date) FROM cs2_portfolio;")
        min_date, max_date = cursor.fetchone()
    
    print_header(title)
    
    print(f"Investment portfolio records: {portfolio_count}")
    print(f"Trading decision records: {decision_count}")
    print(f"Analysis signal records: {signal_count}")
    
    if min_date and max_date:
        print(f"Date range: {min_date[:10]} to {max_date[:10]}")
    
    conn.close()

def main():
    """Main function"""
    exp_name = None
    args = []
    
    # Find --exp parameter
    i = 0
    while i < len(sys.argv):
        if sys.argv[i] == '--exp' and i + 1 < len(sys.argv):
            exp_name = sys.argv[i + 1]
            i += 2
        else:
            args.append(sys.argv[i])
            i += 1
    
    # If the first parameter is not a command, it may be the experiment name
    if len(args) > 1 and args[1] not in ['portfolios', 'positions', 'summary', 'list', 'experiments', 'daily', 'thinking']:
        if not exp_name:  # If not specified by --exp, it is the experiment name
            exp_name = args[1]
            args = [args[0]] + args[2:]  # Remove the experiment name, keep the subsequent parameters
    
    # Process list command (no experiment name required)
    if len(args) > 1 and (args[1] == 'list' or args[1] == 'experiments'):
        list_experiments()
        return
    
    # If no experiment name is specified, list all experiments and prompt
    if not exp_name:
        print("no exp name specified")
        list_experiments()
        return
    
    # Verify if the experiment name exists
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    config_id = get_config_id_by_exp_name(cursor, exp_name)
    conn.close()
    
    if not config_id:
        print(f"Experiment '{exp_name}' does not exist\n")
        list_experiments()
        return
    
    # Execute the corresponding command
    if len(args) == 1:
        # Display summary and latest positions
        view_summary(exp_name)
        view_portfolios(exp_name)
        view_latest_positions(exp_name)
    elif args[1] == 'portfolios':
        view_portfolios(exp_name)
    elif args[1] == 'positions':
        view_latest_positions(exp_name)
    elif args[1] == 'daily':
        # Support specifying date: python view.py EXP_NAME daily 2025-10-01
        target_date = args[2] if len(args) > 2 else None
        view_daily_positions(exp_name, target_date)
    elif args[1] == 'thinking':
        thinking_command(exp_name)
    elif args[1] == 'summary':
        view_summary(exp_name)
    else:
        print("Usage:")
        print("  python view.py EXP_NAME                      # View all information of specified experiment")
        print("  python view.py EXP_NAME portfolios          # View portfolios")
        print("  python view.py EXP_NAME positions           # View latest positions")
        print("  python view.py EXP_NAME daily               # View daily portfolios and export CSV")
        print("  python view.py EXP_NAME daily DATE          # View portfolios of specified date")
        print("  python view.py EXP_NAME thinking            # Export thinking process JSON file")
        print("  python view.py EXP_NAME summary             # View data summary")
        print("  python view.py list                         # List all experiments")
        print("\nOr use --exp parameter:")
        print("  python view.py --exp EXP_NAME portfolios")
        print("  python view.py --exp EXP_NAME daily")
        print("  python view.py --exp EXP_NAME daily 2025-10-01")
        print("  python view.py --exp EXP_NAME thinking")


if __name__ == "__main__":
    main()
