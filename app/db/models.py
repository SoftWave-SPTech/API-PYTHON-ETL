from decimal import Decimal

from sqlalchemy import Column, DateTime, Integer, Numeric, String, UniqueConstraint, func, Date, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Transacao(Base):
    __tablename__ = "transacao"

    id = Column(Integer, primary_key=True, autoincrement=True)
    usuario_id = Column(Integer, nullable=False)
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


class ImportacaoHistorico(Base):
    __tablename__ = "importacao_historico"

    id = Column(Integer, primary_key=True, autoincrement=True)
    usuario_id = Column(Integer, nullable=False)
    tipo = Column(String(50), nullable=False)
    arquivo = Column(String(255), nullable=False)
    data = Column(DateTime, server_default=func.current_timestamp())
    status = Column(String(20), nullable=False, default="concluido")
    registros = Column(Integer, nullable=False, default=0)
    novos = Column(Integer, nullable=False, default=0)
    atualizados = Column(Integer, nullable=False, default=0)
    erros = Column(Integer, nullable=False, default=0)
