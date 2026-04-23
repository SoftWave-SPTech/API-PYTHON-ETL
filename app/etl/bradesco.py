from datetime import datetime
from decimal import Decimal

import pandas as pd

from app.schemas import TipoTransacao, TransacaoNormalizada


def _eh_data_valida(texto: str) -> bool:
    try:
        datetime.strptime(texto, "%d/%m/%Y")
        return True
    except Exception:
        return False


def _limpar_data(data_str: str) -> str:
    return datetime.strptime(data_str, "%d/%m/%Y").strftime("%d/%m/%Y")


def _limpar_valor(valor: str) -> float | None:
    if not valor or str(valor).strip() == "":
        return None
    bruto = str(valor).strip()
    normalizado = bruto.replace(".", "").replace(",", ".")
    try:
        return float(normalizado)
    except ValueError:
        return None


def _limpar_texto(texto: str) -> str:
    return (texto or "").strip()


def _ler_csv_bradesco(conteudo: bytes) -> pd.DataFrame:
    for encoding in ("utf-8-sig", "latin1"):
        for sep in (";", ","):
            try:
                return pd.read_csv(
                    pd.io.common.BytesIO(conteudo),
                    encoding=encoding,
                    sep=sep,
                    dtype=str,
                )
            except Exception:
                continue
    raise ValueError("Bradesco: não foi possível ler o CSV.")


def extrair_dataframe_bradesco_csv(conteudo: bytes) -> pd.DataFrame:
    df = _ler_csv_bradesco(conteudo).copy()
    df.columns = [str(col).strip().lower() for col in df.columns]

    rename_map = {
        "data": "data_pagamento",
        "historico": "descricao",
        "histórico": "descricao",
        "tipo": "tipo_bradesco",
        "valor": "valor",
    }
    df = df.rename(columns=rename_map)

    obrigatorias = ["data_pagamento", "descricao", "tipo_bradesco", "valor"]
    faltando = set(obrigatorias) - set(df.columns)
    if faltando:
        raise ValueError(f"Bradesco: colunas obrigatórias ausentes: {sorted(faltando)}")

    df = df[obrigatorias].copy()
    df["data_pagamento"] = df["data_pagamento"].astype(str).str.strip()
    df["descricao"] = df["descricao"].astype(str).str.strip()
    df["tipo_bradesco"] = df["tipo_bradesco"].astype(str).str.strip().str.upper()
    df["valor"] = df["valor"].apply(_limpar_valor)

    df = df[df["data_pagamento"].apply(_eh_data_valida)]
    df = df.dropna(subset=["valor"])
    df = df[df["descricao"].str.upper() != "SALDO ANTERIOR"]

    def _mapear_tipo(tipo_bradesco: str, valor: float) -> str | None:
        if tipo_bradesco == "CREDITO":
            return TipoTransacao.receita.value
        if tipo_bradesco == "DEBITO":
            return TipoTransacao.despesa.value
        if valor > 0:
            return TipoTransacao.receita.value
        if valor < 0:
            return TipoTransacao.despesa.value
        return None

    df["tipo"] = df.apply(lambda row: _mapear_tipo(row["tipo_bradesco"], row["valor"]), axis=1)
    df = df.dropna(subset=["tipo"])
    df["data_pagamento"] = df["data_pagamento"].apply(_limpar_data)
    df["valor"] = df["valor"].abs().round(2)

    return df[["data_pagamento", "descricao", "valor", "tipo"]].drop_duplicates().reset_index(drop=True)


def extrair_bradesco_csv(conteudo: bytes) -> list[TransacaoNormalizada]:
    df = extrair_dataframe_bradesco_csv(conteudo)
    itens: list[TransacaoNormalizada] = []
    for _, row in df.iterrows():
        itens.append(
            TransacaoNormalizada(
                data_pagamento=row["data_pagamento"],
                descricao=str(row["descricao"])[:255],
                tipo=TipoTransacao(row["tipo"]),
                valor=Decimal(str(row["valor"])).quantize(Decimal("0.01")),
            )
        )
    return itens