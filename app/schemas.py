from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field


class Banco(str, Enum):
    c6 = "c6"
    bradesco = "bradesco"
    itau = "itau"


class TipoTransacao(str, Enum):
    receita = "receita"
    despesa = "despesa"


class TransacaoNormalizada(BaseModel):
    data_pagamento: str = Field(..., description="YYYY-MM-DD (CSV) ou DD/MM/AAAA (PDF Itaú)")
    descricao: str
    tipo: TipoTransacao
    valor: Decimal = Field(..., description="Valor absoluto, sempre positivo")


class LinhaConciliacao(BaseModel):
    transacao: TransacaoNormalizada
    ja_existia: bool
    inserida: bool
    transacao_id: int | None = None
    usuario_id: int | None = None


class ResultadoEtl(BaseModel):
    banco: Banco
    arquivo_origem: str
    total_extraido: int
    duplicatas_ignoradas: int
    inseridas: int
    linhas: list[LinhaConciliacao]
