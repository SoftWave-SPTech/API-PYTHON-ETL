from decimal import Decimal

from sqlalchemy import Column, DateTime, Integer, Numeric, String, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Transacao(Base):
    __tablename__ = "transacoes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_pagamento = Column(String(10), nullable=False)
    descricao = Column(String(255), nullable=False)
    tipo = Column(String(20), nullable=False)
    valor = Column(Numeric(12, 2), nullable=False)
    arquivo_origem = Column(String(255), nullable=False)
    data_insercao = Column(DateTime, server_default=func.current_timestamp())
    
    #  Constraint UNIQUE para evitar duplicatas
    __table_args__ = (
        UniqueConstraint('data_pagamento', 'descricao', 'tipo', 'valor', 'arquivo_origem',
                        name='uk_transacao'),
    )
