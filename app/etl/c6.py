import io
from decimal import Decimal

import pandas as pd

from app.schemas import TipoTransacao, TransacaoNormalizada


def _ler_csv_c6(conteudo: bytes) -> pd.DataFrame:
    """
    O extrato do C6 Bank possui 8 linhas de metadados antes do header.
    Colunas: Data Lançamento, Data Contábil, Título, Descrição, Entrada(R$), Saída(R$), Saldo do Dia(R$)
    """
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return pd.read_csv(
                io.BytesIO(conteudo),
                encoding=enc,
                sep=",",
                skiprows=8,
            )
        except Exception:
            continue
    raise ValueError("C6: não foi possível ler o CSV.")


def extrair_dataframe_c6_csv(conteudo: bytes) -> pd.DataFrame:
    df = _ler_csv_c6(conteudo).copy()
    df.columns = [str(col).strip() for col in df.columns]

    rename_map: dict[str, str] = {}
    for col in df.columns:
        col_lower = col.lower()
        if "lançamento" in col_lower or "lancamento" in col_lower:
            rename_map[col] = "data_pagamento"
        elif "descrição" in col_lower or "descricao" in col_lower:
            rename_map[col] = "descricao"
        elif "entrada" in col_lower:
            rename_map[col] = "entrada"
        elif "saída" in col_lower or "saida" in col_lower:
            rename_map[col] = "saida"

    df = df.rename(columns=rename_map)

    obrigatorias = ["data_pagamento", "descricao", "entrada", "saida"]
    faltando = set(obrigatorias) - set(df.columns)
    if faltando:
        raise ValueError(f"C6: colunas obrigatórias ausentes: {sorted(faltando)}")

    df = df[obrigatorias].copy()

    df["descricao"] = df["descricao"].astype(str).str.strip()

    df["data_pagamento"] = pd.to_datetime(
        df["data_pagamento"], dayfirst=True, errors="coerce"
    ).dt.strftime("%d/%m/%Y")

    def _parse_valor(serie: pd.Series) -> pd.Series:
        return (
            serie.astype(str)
            .str.replace(r"\s", "", regex=True)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.replace(r"[^\d\.-]", "", regex=True)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )

    df["entrada"] = _parse_valor(df["entrada"])
    df["saida"] = _parse_valor(df["saida"])

    # Cada linha tem entrada OU saída — nunca os dois simultaneamente.
    # entrada > 0 → receita   |   saida > 0 → despesa
    df["tipo"] = df.apply(
        lambda r: TipoTransacao.receita.value if r["entrada"] > 0 else TipoTransacao.despesa.value,
        axis=1,
    )
    df["valor"] = df.apply(
        lambda r: r["entrada"] if r["entrada"] > 0 else r["saida"],
        axis=1,
    ).round(2)

    df = df[["data_pagamento", "descricao", "tipo", "valor"]]
    df = df[df["valor"] > 0]  # descarta linhas com ambos zero
    df = df.dropna(subset=["data_pagamento", "descricao"]).drop_duplicates().reset_index(drop=True)

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