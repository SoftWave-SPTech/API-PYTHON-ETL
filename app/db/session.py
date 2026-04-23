from collections.abc import Generator
import logging

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings

logger = logging.getLogger(__name__)

try:
    engine = create_engine(
        settings.resolved_database_url,
        pool_pre_ping=True,
        connect_args={
            "connection_timeout": settings.db_connection_timeout,
            "autocommit": settings.db_autocommit,
            "charset": settings.db_charset,
            "use_pure": settings.db_use_pure,
        },
    )
    # Testa a conexão ao inicializar
    with engine.connect() as connection:
        logger.info("✓ Conexão com banco de dados estabelecida com sucesso")
except Exception as e:
    logger.error(f"✗ Erro ao conectar ao banco de dados: {e}")
    logger.error(f"  DATABASE_URL: {settings.resolved_database_url}")
    raise

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    class_=Session,
)


def get_session() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

