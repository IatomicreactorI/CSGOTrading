"""Clear CS2 experiment database data"""
"""Example: python clear.py T-ds"""
import sqlite3
import os
import sys
import traceback
from database.cs2_sqlite_setup import CS2_DB_PATH


def _confirm_deletion(exp_name: str) -> bool:
    """Confirm whether to delete experiment data"""
    print("Warning: This operation will permanently delete experiment data and cannot be recovered!")
    print(f"Experiment name: {exp_name}\n")
    
    while True:
        response = input("Confirm deletion? Please enter the experiment name to confirm (enter 'cancel' to cancel): ").strip()
        if response.lower() == 'cancel':
            return False
        if response == exp_name:
            return True
        print(f"Input does not match. Please enter '{exp_name}' to confirm deletion, or enter 'cancel' to cancel.")

def clear_cs2_experiment(exp_name: str):
    """Clear all data for the specified CS2 experiment"""
    db_path = CS2_DB_PATH
    
    # Check if database file exists first
    if not os.path.exists(db_path):
        print(f"Database file does not exist: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get config_id
        cursor.execute("SELECT id FROM cs2_config WHERE exp_name = ?", (exp_name,))
        row = cursor.fetchone()
        if not row:
            print(f"Experiment '{exp_name}' does not exist")
            return
        config_id = row[0]
        
        # Get portfolio IDs and count statistics
        cursor.execute("SELECT id FROM cs2_portfolio WHERE config_id = ?", (config_id,))
        portfolio_ids = [row[0] for row in cursor.fetchall()]
        
        if portfolio_ids:
            placeholders = ','.join(['?' for _ in portfolio_ids])
            cursor.execute(f"SELECT COUNT(*) FROM cs2_decision WHERE portfolio_id IN ({placeholders})", portfolio_ids)
            decision_count = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(*) FROM cs2_signal WHERE portfolio_id IN ({placeholders})", portfolio_ids)
            signal_count = cursor.fetchone()[0]
        else:
            decision_count = signal_count = 0
        
        portfolio_count = len(portfolio_ids)
        
        print(f"Ready to delete data for experiment '{exp_name}':")
        print(f"  - Portfolio records: {portfolio_count}")
        print(f"  - Trading decision records: {decision_count}")
        print(f"  - Analysis signal records: {signal_count}\n")
        
        if not _confirm_deletion(exp_name):
            print("Operation cancelled")
            return
        
        # Delete data
        if portfolio_ids:
            placeholders = ','.join(['?' for _ in portfolio_ids])
            cursor.execute(f"DELETE FROM cs2_decision WHERE portfolio_id IN ({placeholders})", portfolio_ids)
            cursor.execute(f"DELETE FROM cs2_signal WHERE portfolio_id IN ({placeholders})", portfolio_ids)
        
        cursor.execute("DELETE FROM cs2_portfolio WHERE config_id = ?", (config_id,))
        cursor.execute("DELETE FROM cs2_config WHERE id = ?", (config_id,))
        conn.commit()
        
        print(f"\nSuccessfully cleared all data for experiment '{exp_name}'")
        print(f"   - Deleted {portfolio_count} portfolio records")
        print(f"   - Deleted {decision_count} trading decision records")
        print(f"   - Deleted {signal_count} analysis signal records")
        print(f"   - Deleted configuration record")
        
    except Exception as e:
        conn.rollback()
        print(f"Deletion failed: {e}")
        traceback.print_exc()
    finally:
        conn.close()

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Error: must provide experiment name")
        sys.exit(1)
    exp_name = sys.argv[1]
    print(f"Clearing experiment: {exp_name}")
    print("=" * 80 + "\n")
    clear_cs2_experiment(exp_name)

if __name__ == "__main__":
    main()