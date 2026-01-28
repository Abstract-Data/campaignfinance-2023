#!/usr/bin/env python3
"""
PostgreSQL configuration for the unified campaign finance database.
"""

import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()


class PostgresConfig:
    """PostgreSQL configuration settings."""
    
    def __init__(self):
        # Default PostgreSQL connection settings
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = os.getenv("POSTGRES_DB", "campaign_finance")
        self.username = os.getenv("POSTGRES_USER", "johneakin")
        self.password = os.getenv("POSTGRES_PASSWORD", "")
        
        # Connection pool settings
        self.pool_size = int(os.getenv("POSTGRES_POOL_SIZE", "10"))
        self.max_overflow = int(os.getenv("POSTGRES_MAX_OVERFLOW", "20"))
        self.pool_timeout = int(os.getenv("POSTGRES_POOL_TIMEOUT", "30"))
        self.pool_recycle = int(os.getenv("POSTGRES_POOL_RECYCLE", "3600"))
    
    @property
    def database_url(self) -> str:
        """Generate the PostgreSQL database URL."""
        if self.password:
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            return f"postgresql://{self.username}@{self.host}:{self.port}/{self.database}"
    
    def get_connection_url(self, include_password: bool = True) -> str:
        """Get connection URL with optional password inclusion."""
        if include_password and self.password:
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            return f"postgresql://{self.username}@{self.host}:{self.port}/{self.database}"
    
    def validate_connection(self) -> bool:
        """Validate that the PostgreSQL connection settings are correct."""
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            conn.close()
            return True
        except Exception as e:
            print(f"PostgreSQL connection validation failed: {e}")
            return False
    
    def create_database_if_not_exists(self) -> bool:
        """Create the database if it doesn't exist."""
        try:
            import psycopg2
            # Connect to default postgres database to create our database
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database="postgres",  # Connect to default database
                user=self.username,
                password=self.password
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.database,))
            exists = cursor.fetchone()
            
            if not exists:
                cursor.execute(f"CREATE DATABASE {self.database}")
                print(f"Created database: {self.database}")
            else:
                print(f"Database {self.database} already exists")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Error creating database: {e}")
            return False
    
    def print_connection_info(self):
        """Print connection information (without password)."""
        print(f"PostgreSQL Configuration:")
        print(f"  Host: {self.host}")
        print(f"  Port: {self.port}")
        print(f"  Database: {self.database}")
        print(f"  Username: {self.username}")
        print(f"  Password: {'*' * len(self.password) if self.password else 'None'}")
        print(f"  Connection URL: {self.get_connection_url(include_password=False)}")


def create_postgres_database_manager():
    """Create a PostgreSQL database manager instance."""
    from app.core.unified_database import UnifiedDatabaseManager
    
    config = PostgresConfig()
    
    # Validate connection (database should already exist)
    if not config.validate_connection():
        raise Exception("Failed to connect to PostgreSQL database")
    
    # Create database manager with PostgreSQL URL
    return UnifiedDatabaseManager(config.database_url)


def get_postgres_config() -> PostgresConfig:
    """Get the PostgreSQL configuration."""
    return PostgresConfig()


# Example usage and setup
if __name__ == "__main__":
    config = PostgresConfig()
    config.print_connection_info()
    
    if config.validate_connection():
        print("✅ PostgreSQL connection successful!")
    else:
        print("❌ PostgreSQL connection failed!")
        print("\nTo set up PostgreSQL:")
        print("1. Install PostgreSQL: brew install postgresql")
        print("2. Start PostgreSQL: brew services start postgresql")
        print("3. Create a .env file with your settings:")
        print("   POSTGRES_HOST=localhost")
        print("   POSTGRES_PORT=5432")
        print("   POSTGRES_DB=campaign_finance")
        print("   POSTGRES_USER=postgres")
        print("   POSTGRES_PASSWORD=your_password") 