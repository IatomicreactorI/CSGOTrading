from database.cs2_sqlite_helper import CS2SQLiteDB
from database.cs2_sqlite_setup import init_cs2_database
from util.logger import logger

# global variable that will be set in CS2 analysis scripts
cs2_db = None

def cs2_db_initialize(use_local_db: bool = False):
    """Initialize the database connection based on the local-db flag."""
    global cs2_db
    if use_local_db:
        _db = CS2SQLiteDB()
        init_cs2_database()  # Ensure the table structure is created
        logger.info("CS2 SQLite database initialized")
    else:
        _db = CS2SupabaseDB()
        logger.info("CS2 Supabase database initialized")
    cs2_db = _db
    
def get_cs2_db():
    """Get the database instance."""
    return cs2_db 