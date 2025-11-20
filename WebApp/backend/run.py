#!/usr/bin/env python3
"""
Chinook Store API startup script
"""
import uvicorn
from app.db import init_db, get_db_info
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main application entry point"""
    try:
        # Initialize database
        logger.info("Initializing database...")
        init_db()
        
        # Check database connection
        db_info = get_db_info()
        logger.info(f"Database status: {db_info}")
        
        if db_info["status"] != "connected":
            logger.error("Database connection failed!")
            return
        
        # Start the server
        logger.info("Starting Chinook Store API...")
        uvicorn.run(
            "app.main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
        
    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        raise

if __name__ == "__main__":
    main()