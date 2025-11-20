from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

# Database engine configuration
engine_config = {
    "pool_pre_ping": True,
    "pool_recycle": 3600,  # Recycle connections every hour
    "pool_size": 10,       # Connection pool size
    "max_overflow": 20,    # Maximum overflow connections
    "poolclass": QueuePool,
    "echo": os.getenv("SQL_ECHO", "false").lower() == "true",  # SQL logging
}

# Create engine
engine = create_engine(DATABASE_URL, **engine_config)

# Session configuration
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False
)

Base = declarative_base()


# Event listeners for connection pool monitoring
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Set SQLite pragmas for better performance (if using SQLite)"""
    if "sqlite" in DATABASE_URL.lower():
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=1000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.close()


@event.listens_for(engine, "checkout")
def ping_connection(dbapi_connection, connection_record, connection_proxy):
    """Ensure connections are alive when checked out from pool"""
    logger.debug("Connection checked out from pool")


def get_db():
    """Database dependency for FastAPI"""
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()


def init_db():
    """Initialize database tables"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        raise


def get_db_info():
    """Get database connection information"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            return {
                "status": "connected",
                "database_url": DATABASE_URL.split("@")[-1] if "@" in DATABASE_URL else "local",
                "pool_size": engine.pool.size(),
                "checked_out_connections": engine.pool.checkedout(),
            }
    except Exception as e:
        logger.error(f"Database connection check failed: {str(e)}")
        return {"status": "error", "error": str(e)}
