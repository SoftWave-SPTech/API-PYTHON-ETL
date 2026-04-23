from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import io
import csv

from app.db.models import Transacao
from app.db.session import get_session
from app.etl import extrair_bradesco_csv, extrair_c6_csv, extrair_itau_pdf
from app.etl.bradesco import extrair_dataframe_bradesco_csv
from app.etl.c6 import extrair_dataframe_c6_csv
from app.schemas import Banco, ResultadoEtl
from app.services.conciliacao import processar_com_conciliacao

router = APIRouter(prefix="/etl", tags=["etl"])


def _validar_extensao(nome_arquivo: str, banco: Banco) -> None:
    if banco in (Banco.c6, Banco.bradesco) and not nome_arquivo.lower().endswith(".csv"):
        detalhe = "C6: envie um arquivo .csv" if banco == Banco.c6 else "Bradesco: envie um arquivo .csv"
        raise HTTPException(status_code=400, detail=detalhe)
    if banco == Banco.itau and not nome_arquivo.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Itaú: envie um arquivo .pdf")


@router.post("/upload", response_model=ResultadoEtl)
async def upload_extrato(
    banco: Banco,
    arquivo: UploadFile = File(...),
    persistir: bool = False,
    session: Session = Depends(get_session),
):
    nome = arquivo.filename or "upload"
    conteudo = await arquivo.read()
    if not conteudo:
        raise HTTPException(status_code=400, detail="Arquivo vazio.")

    try:
        _validar_extensao(nome, banco)
        if banco == Banco.c6:
            itens = extrair_c6_csv(conteudo)
        elif banco == Banco.bradesco:
            itens = extrair_bradesco_csv(conteudo)
        else:
            itens = extrair_itau_pdf(conteudo)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Falha ao processar arquivo: {e}") from e

    try:
        return processar_com_conciliacao(session, banco, nome, itens, persistir)
    except SQLAlchemyError as e:
        session.rollback()
        raise HTTPException(
            status_code=503,
            detail=f"Falha de banco de dados ao persistir conciliação: {e}",
        ) from e
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Falha na conciliação: {e}") from e


@router.get("/db-health")
def db_health(session: Session = Depends(get_session)):
    try:
        resultado = session.execute(text("SELECT 1")).scalar()
        return {"status": "ok", "database": "conectado", "select_1": int(resultado or 0)}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=503, detail=f"Falha de conexão com banco: {e}") from e

@router.post("/preview")
async def preview_extrato(
    banco: Banco,
    arquivo: UploadFile = File(...),
    limite: int = 20,
):
    nome = arquivo.filename or "upload"
    conteudo = await arquivo.read()
    if not conteudo:
        raise HTTPException(status_code=400, detail="Arquivo vazio.")
    if limite < 1 or limite > 200:
        raise HTTPException(status_code=400, detail="Parâmetro 'limite' deve estar entre 1 e 200.")

    try:
        _validar_extensao(nome, banco)
        if banco == Banco.c6:
            df = extrair_dataframe_c6_csv(conteudo)
        elif banco == Banco.bradesco:
            df = extrair_dataframe_bradesco_csv(conteudo)
        else:
            raise HTTPException(
                status_code=400,
                detail="Preview de DataFrame disponível apenas para C6 e Bradesco (CSV).",
            )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Falha ao processar arquivo: {e}") from e

    return {
        "banco": banco.value,
        "arquivo_origem": nome,
        "total_linhas": int(len(df)),
        "colunas": [str(col) for col in df.columns.tolist()],
        "amostra": df.head(limite).to_dict(orient="records"),
    }


@router.get("/extrato/csv")
def exportar_extrato_csv(session: Session = Depends(get_session)):
    """
    Exporta todas as transações da tabela em formato CSV.
    
    Retorna um arquivo CSV com as colunas: id, data_pagamento, descricao, tipo, valor, arquivo_origem, data_insercao
    """
    try:
        # Busca todas as transações da tabela
        transacoes = session.query(Transacao).all()
        
        if not transacoes:
            raise HTTPException(status_code=404, detail="Nenhuma transação encontrada na tabela.")
        
        # Cria um buffer em memória para o CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Escreve o cabeçalho
        writer.writerow([
            "id",
            "data_pagamento",
            "descricao",
            "tipo",
            "valor",
            "arquivo_origem",
            "data_insercao"
        ])
        
        # Escreve as linhas de dados
        for transacao in transacoes:
            writer.writerow([
                transacao.id,
                transacao.data_pagamento,
                transacao.descricao,
                transacao.tipo,
                str(transacao.valor),
                transacao.arquivo_origem,
                transacao.data_insercao.strftime("%d/%m/%Y %H:%M:%S") if transacao.data_insercao else ""
            ])
        
        # Retorna o CSV como um arquivo para download
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=extrato.csv"}
        )
    
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        raise HTTPException(status_code=503, detail=f"Falha de banco de dados: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao exportar extrato: {e}") from e
