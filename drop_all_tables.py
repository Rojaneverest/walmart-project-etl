"""
Script to drop all tables in the database and start fresh.
"""
from sqlalchemy import create_engine, text
from config import get_connection_string

def drop_all_tables():
    """Drop all tables in the database."""
    print("Dropping all tables in the database...")
    
    # Get database engine
    connection_string = get_connection_string()
    engine = create_engine(connection_string)
    
    # Drop all tables
    with engine.begin() as conn:
        # Drop staging tables first
        conn.execute(text("DROP TABLE IF EXISTS stg_sales CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_inventory CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_returns CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_product CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_customer CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_store CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_supplier CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_return_reason CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS stg_date CASCADE"))
        
        # Drop ODS tables
        conn.execute(text("DROP TABLE IF EXISTS ods_sales CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_inventory CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_returns CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_product CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_customer CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_store CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_supplier CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_return_reason CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS ods_date CASCADE"))
        
        # Drop target tables
        conn.execute(text("DROP TABLE IF EXISTS tgt_fact_sales CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_fact_inventory CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_fact_returns CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_product CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_customer CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_store CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_supplier CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_return_reason CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS tgt_dim_date CASCADE"))
    
    print("All tables dropped successfully!")

if __name__ == "__main__":
    drop_all_tables()
