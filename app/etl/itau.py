import io
import re
from decimal import Decimal

import pdfplumber

from app.schemas import TipoTransacao, TransacaoNormalizada


def parse_brl_valor(valor_str: str) -> float:
    """Converte valor BRL string para float (mesma lógica de itau_etl_pdf)."""
    normalizado = valor_str.replace(".", "").replace(",", ".")
    return float(normalizado)


def extrair_dados_pdf_itau(conteudo_pdf: bytes) -> list[TransacaoNormalizada]:

    transacoes: list[TransacaoNormalizada] = []
    padrao_data = re.compile(r"^(\d{2}/\d{2}/\d{4})\s+(.*)$")
    padrao_valor_final = re.compile(r"(-?\d{1,3}(?:\.\d{3})*,\d{2})\s*$")

    with pdfplumber.open(io.BytesIO(conteudo_pdf)) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text() or ""
            for linha in texto.split("\n"):
                linha = re.sub(r"\s+", " ", linha).strip()
                m_data = padrao_data.match(linha)
                if not m_data:
                    continue
                data_str = m_data.group(1)
                resto = m_data.group(2).strip()
                if "lançamentos" in resto.lower() and "valor" in resto.lower():
                    continue
                m_valor = padrao_valor_final.search(resto)
                if not m_valor:
                    continue
                valor_str = m_valor.group(1)
                valor = parse_brl_valor(valor_str)
                descricao = resto[: m_valor.start()].strip()
                if "saldo do dia" in descricao.lower():
                    continue
                tipo = TipoTransacao.despesa if valor < 0 else TipoTransacao.receita
                transacoes.append(
                    TransacaoNormalizada(
                        data_pagamento=data_str,
                        descricao=descricao[:255],
                        tipo=tipo,
                        valor=Decimal(str(abs(valor))).quantize(Decimal("0.01")),
                    )
                )
    return transacoes


def extrair_itau_pdf(conteudo: bytes) -> list[TransacaoNormalizada]:
    return extrair_dados_pdf_itau(conteudo)
