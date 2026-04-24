from decimal import Decimal

from sqlalchemy import Column, DateTime, Integer, Numeric, String, UniqueConstraint, func, Date, ForeignKey, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Transacao(Base):
    __tablename__ = "transacao"

    id = Column(Integer, primary_key=True, autoincrement=True)
    honorario_id = Column(Integer, nullable=True)
    titulo = Column(String(150))
    valor = Column(Numeric(10, 2))
    tipo = Column(String(50))
    status_financeiro = Column(String(50))
    status_aprovacao = Column(String(50))
    data_emissao = Column(Date)
    data_vencimento = Column(Date)
    data_pagamento = Column(String(10), nullable=False)
    descricao = Column(Text)
    observacoes = Column(Text)
    contraparte = Column(String(150))
    arquivo_origem = Column(String(255), nullable=True)
    data_insercao = Column(DateTime, server_default=func.current_timestamp())
