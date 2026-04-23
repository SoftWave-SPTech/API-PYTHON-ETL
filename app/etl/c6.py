import io
from decimal import Decimal

import pandas as pd

from app.schemas import TipoTransacao, TransacaoNormalizada


def _ler_csv_c6(conteudo: bytes) -> pd.DataFrame:
    for params in (
        {"encoding": "utf-8", "sep": ","},
        {"encoding": "latin1", "sep": ","},
        {"encoding": "utf-8", "sep": ";"},
        {"encoding": "latin1", "sep": ";"},
    ):
        try:
            return pd.read_csv(io.BytesIO(conteudo), **params)
        except Exception:
            continue
    raise ValueError("C6: não foi possível ler o CSV.")


def extrair_dataframe_c6_csv(conteudo: bytes) -> pd.DataFrame:
    df = _ler_csv_c6(conteudo).copy()
    df.columns = [str(col).strip().lower() for col in df.columns]

    rename_map = {}
    for col in df.columns:
        if "data" in col:
            rename_map[col] = "data_pagamento"
        elif "desc" in col or "hist" in col:
            rename_map[col] = "descricao"
        elif "valor" in col:
            rename_map[col] = "valor"
    df = df.rename(columns=rename_map)

    obrigatorias = ["data_pagamento", "descricao", "valor"]
    faltando = set(obrigatorias) - set(df.columns)
    if faltando:
        raise ValueError(f"C6: colunas obrigatórias ausentes: {sorted(faltando)}")

    df = df[obrigatorias].copy()
    df["descricao"] = df["descricao"].astype(str).str.strip()
    df["data_pagamento"] = pd.to_datetime(df["data_pagamento"], errors="coerce").dt.strftime("%d/%m/%Y")
    df["valor"] = (
        df["valor"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(r"[^\d\.-]", "", regex=True)
    )
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    df = df.dropna(subset=["data_pagamento", "descricao", "valor"])
    df["tipo"] = df["valor"].apply(lambda v: TipoTransacao.receita.value if v >= 0 else TipoTransacao.despesa.value)
    df["valor"] = df["valor"].abs().round(2)
    df = df.drop_duplicates().reset_index(drop=True)
    return df


def extrair_c6_csv(conteudo: bytes) -> list[TransacaoNormalizada]:
    df = extrair_dataframe_c6_csv(conteudo)
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
