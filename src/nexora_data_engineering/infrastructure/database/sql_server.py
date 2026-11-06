"""
MODULE: SQL Server Connection Management

ARCHITECTURAL LAYER:
Infrastructure Layer / Database Connectivity

PURPOSE:
Creates and manages connections to the source SQL Server
database using SQLAlchemy and the pyodbc driver.

RESPONSIBILITIES:
- Build the SQL Server connection URL.
- Load credentials and connection settings.
- Create and cache the SQLAlchemy Engine.
- Configure database connection pooling.
- Verify source database connectivity.
- Dispose of database connections when required.

DATA ENGINEERING ROLE:
Provides the database connectivity foundation used by
schema discovery and future extraction services.

The SQLAlchemy Engine will be reused by components that
need to query NexoraEnterprise.

DESIGN CONSIDERATIONS:
- Connection credentials come from application settings.
- Connection pooling improves connection reuse.
- pool_pre_ping helps detect stale connections.
- Connection lifecycle management should be explicit.
- Production environments must use appropriate certificate
  validation and secure authentication.
- This module manages connectivity, not ingestion logic.
"""

from functools import lru_cache

from sqlalchemy import URL, Engine, create_engine, text

from nexora_data_engineering.config.settings import get_settings


@lru_cache
def get_sql_server_engine() -> Engine:
    """
    Create and cache the SQL Server SQLAlchemy engine.

    The engine manages connection pooling and reuses database
    connections instead of opening a new connection for every query.
    """
    settings = get_settings()

    connection_url = URL.create(
        drivername="mssql+pyodbc",
        username=settings.db_username,
        password=settings.db_password.get_secret_value(),
        host=settings.db_server,
        database=settings.db_database,
        query={
            "driver": settings.db_driver,
            "Encrypt": "yes" if settings.db_encrypt else "no",
            "TrustServerCertificate": ("yes" if settings.db_trust_server_certificate else "no"),
        },
    )

    engine = create_engine(
        connection_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        connect_args={"timeout": 10},
    )

    return engine


def test_database_connection() -> dict[str, str]:
    """Test connectivity and return basic database identity information."""
    engine = get_sql_server_engine()

    with engine.connect() as connection:
        result = (
            connection.execute(
                text(
                    """
                SELECT
                    DB_NAME() AS database_name,
                    SUSER_SNAME() AS login_name,
                    @@SERVERNAME AS server_name
                """
                )
            )
            .mappings()
            .one()
        )

        return dict(result)


def dispose_database_engine() -> None:
    """Close all pooled database connections."""
    get_sql_server_engine.cache_clear()
