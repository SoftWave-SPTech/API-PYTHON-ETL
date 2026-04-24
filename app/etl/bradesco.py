from datetime import datetime
from decimal import Decimal
import pandas as pd
import io
import logging
from app.schemas import TipoTransacao, TransacaoNormalizada

logger = logging.getLogger(__name__)


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


def extrair_dataframe_bradesco_csv(conteudo: bytes) -> pd.DataFrame:
    """
    Processa o extrato do Bradesco linha por linha.
    Formato esperado (após a primeira linha de info):
    Data;Histórico;Docto.;Crédito (R$);Débito (R$);Saldo (R$)
    """
    dados = []
    
    # Converte bytes para string
    try:
        conteudo_str = conteudo.decode('utf-8-sig')
    except:
        conteudo_str = conteudo.decode('latin1')
    
    linhas = conteudo_str.strip().split('\n')
    logger.info(f"Total de linhas no arquivo: {len(linhas)}")
    
    # Pula a primeira linha (informações da conta)
    # e a segunda linha (cabeçalho)
    for idx, linha in enumerate(linhas[2:], start=2):
        logger.debug(f"Processando linha {idx}: {linha}")
        
        # Interrompe se encontrar palavras-chave de fim de dados
        if any(palavra in linha.lower() for palavra in ["lancamentos", "filtro", "dados acima", "ultimos", "total", "nao ha"]):
            logger.info(f"Parando na linha {idx}: encontrou palavra-chave de fim")
            break
        
        # Separa as colunas
        colunas = linha.strip().split(";")
        
        # Precisa de pelo menos 5 colunas (Data, Histórico, Docto, Crédito, Débito)
        if len(colunas) < 5:
            logger.debug(f"Pulando linha {idx}: menos de 5 colunas")
            continue
        
        # Extrai e valida data
        data_raw = colunas[0].strip()
        if not _eh_data_valida(data_raw):
            logger.debug(f"Pulando linha {idx}: data inválida '{data_raw}'")
            continue
        
        # Extrai descrição
        descricao = _limpar_texto(colunas[1])
        if not descricao or descricao.upper() == "SALDO ANTERIOR":
            logger.debug(f"Pulando linha {idx}: descrição vazia ou saldo anterior")
            continue
        
        # Extrai crédito e débito
        credito = _limpar_valor(colunas[3])
        debito = _limpar_valor(colunas[4])
        
        # Determina valor e tipo
        if credito and credito > 0:
            valor = credito
            tipo = TipoTransacao.receita.value
        elif debito and debito > 0:
            valor = debito
            tipo = TipoTransacao.despesa.value
        else:
            logger.debug(f"Pulando linha {idx}: crédito={credito}, débito={debito}")
            continue
        
        # Adiciona à lista de dados
        dados.append({
            "data_pagamento": _limpar_data(data_raw),
            "descricao": descricao[:255],
            "valor": round(valor, 2),
            "tipo": tipo
        })
        logger.debug(f"Linha {idx} adicionada: {data_raw}, {descricao}, {valor}, {tipo}")
    
    logger.info(f"Total de transações processadas: {len(dados)}")
    
    if not dados:
        raise ValueError("Bradesco: nenhuma transação válida encontrada no CSV")
    
    # Converte para DataFrame
    df = pd.DataFrame(dados)
    
    # Remove duplicatas e reseta índice
    df = df.drop_duplicates().reset_index(drop=True)
    
    logger.info(f"Após remover duplicatas: {len(df)} linhas")
    return df


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
    logger.info(f"Retornando {len(itens)} transações normalizadas")
    return itens