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
from app.services.conciliacao import garantir_estrutura_transacao, processar_com_conciliacao

router = APIRouter(prefix="/etl", tags=["etl"])


SQL_CREATE_IMPORTACOES_HISTORICO = """
CREATE TABLE IF NOT EXISTS importacao_historico (
    id INT AUTO_INCREMENT PRIMARY KEY,
    usuario_id INT NOT NULL,
    tipo VARCHAR(50) NOT NULL,
    arquivo VARCHAR(255) NOT NULL,
    data TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'concluido',
    registros INT NOT NULL DEFAULT 0,
    novos INT NOT NULL DEFAULT 0,
    atualizados INT NOT NULL DEFAULT 0,
    erros INT NOT NULL DEFAULT 0,
    CONSTRAINT fk_importacao_historico_usuario FOREIGN KEY (usuario_id) REFERENCES usuario(id)
)
"""


def _garantir_usuario_fk_historico(session: Session) -> None:
    cols = session.execute(text("SHOW COLUMNS FROM importacao_historico LIKE 'usuario_id'")).mappings().all()
    if not cols:
        session.execute(text("ALTER TABLE importacao_historico ADD COLUMN usuario_id INT NULL"))
    session.execute(text("ALTER TABLE importacao_historico MODIFY COLUMN usuario_id INT NOT NULL"))

    fk_rows = session.execute(
        text(
            """
            SELECT CONSTRAINT_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'importacao_historico'
              AND COLUMN_NAME = 'usuario_id'
              AND REFERENCED_TABLE_NAME = 'usuario'
              AND REFERENCED_COLUMN_NAME = 'id'
            LIMIT 1
            """
        )
    ).fetchall()
    if not fk_rows:
        session.execute(
            text(
                """
                ALTER TABLE importacao_historico
                ADD CONSTRAINT fk_importacao_historico_usuario
                FOREIGN KEY (usuario_id) REFERENCES usuario(id)
                """
            )
        )


def _garantir_tabela_historico(session: Session) -> None:
    session.execute(text(SQL_CREATE_IMPORTACOES_HISTORICO))
    _garantir_usuario_fk_historico(session)


def _registrar_historico_importacao(
    session: Session,
    banco: Banco,
    resultado: ResultadoEtl,
    usuario_id: int,
) -> None:
    _garantir_tabela_historico(session)
    session.execute(
        text(
            """
            INSERT INTO importacao_historico
            (usuario_id, tipo, arquivo, status, registros, novos, atualizados, erros)
            VALUES
            (:usuario_id, :tipo, :arquivo, :status, :registros, :novos, :atualizados, :erros)
            """
        ),
        {
            "usuario_id": usuario_id,
            "tipo": f"extrato_{banco.value}",
            "arquivo": resultado.arquivo_origem,
            "status": "concluido",
            "registros": int(resultado.total_extraido),
            "novos": int(resultado.inseridas),
            "atualizados": int(resultado.duplicatas_ignoradas),
            "erros": 0,
        },
    )


def _validar_extensao(nome_arquivo: str, banco: Banco) -> None:
    if banco in (Banco.c6, Banco.bradesco) and not nome_arquivo.lower().endswith(".csv"):
        detalhe = "C6: envie um arquivo .csv" if banco == Banco.c6 else "Bradesco: envie um arquivo .csv"
        raise HTTPException(status_code=400, detail=detalhe)
    if banco == Banco.itau and not nome_arquivo.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Itaú: envie um arquivo .pdf")


@router.post("/upload", response_model=ResultadoEtl)
async def upload_extrato(
    banco: Banco,
    usuario_id: int,
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
        resultado = processar_com_conciliacao(
            session,
            banco,
            nome,
            itens,
            persistir,
            usuario_id=usuario_id,
        )
        _registrar_historico_importacao(session, banco, resultado, usuario_id=usuario_id)
        session.commit()
        return resultado
    except SQLAlchemyError as e:
        session.rollback()
        raise HTTPException(
            status_code=503,
            detail=f"Falha de banco de dados ao persistir conciliação: {e}",
        ) from e
    except ValueError as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Falha na conciliação: {e}") from e


@router.get("/importacao/historico")
def listar_historico_importacoes(
    usuario_id: int,
    limit: int = 50,
    session: Session = Depends(get_session),
):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="Parâmetro 'limit' deve estar entre 1 e 500.")
    try:
        _garantir_tabela_historico(session)
        rows = session.execute(
            text(
                """
                SELECT id, tipo, arquivo, data, status, registros, novos, atualizados, erros
                FROM importacao_historico
                WHERE usuario_id = :usuario_id
                ORDER BY id DESC
                LIMIT :limit
                """
            ),
            {"limit": limit, "usuario_id": usuario_id},
        ).mappings().all()
        return {
            "importacoes": [
                {
                    "id": int(r["id"]),
                    "tipo": str(r["tipo"]),
                    "arquivo": str(r["arquivo"]),
                    "data": r["data"].isoformat() if r["data"] else "",
                    "status": str(r["status"]),
                    "registros": int(r["registros"] or 0),
                    "novos": int(r["novos"] or 0),
                    "atualizados": int(r["atualizados"] or 0),
                    "erros": int(r["erros"] or 0),
                }
                for r in rows
            ]
        }
    except SQLAlchemyError as e:
        raise HTTPException(status_code=503, detail=f"Falha de banco de dados: {e}") from e


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
def exportar_extrato_csv(
    usuario_id: int,
    session: Session = Depends(get_session),
):
    """
    Exporta todas as transações da tabela em formato CSV.
    
    Retorna um arquivo CSV com as colunas: id, honorario_id, titulo, valor, tipo, status_financeiro, 
    status_aprovacao, data_emissao, data_vencimento, data_pagamento, descricao, observacoes, 
    contraparte, arquivo_origem, data_insercao
    """
    try:
        garantir_estrutura_transacao(session)
        # Busca somente as transações do usuário informado.
        transacoes = session.query(Transacao).filter(Transacao.usuario_id == usuario_id).all()
        
        if not transacoes:
            raise HTTPException(status_code=404, detail="Nenhuma transação encontrada na tabela.")
        
        # Cria um buffer em memória para o CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Escreve o cabeçalho
        writer.writerow([
            "id",
            "usuario_id",
            "honorario_id",
            "titulo",
            "valor",
            "tipo",
            "status_financeiro",
            "status_aprovacao",
            "data_emissao",
            "data_vencimento",
            "data_pagamento",
            "descricao",
            "observacoes",
            "contraparte",
            "arquivo_origem",
            "data_insercao"
        ])
        
        # Escreve as linhas de dados
        for transacao in transacoes:
            writer.writerow([
                transacao.id,
                transacao.usuario_id or "",
                transacao.honorario_id,
                transacao.titulo or "",
                str(transacao.valor) if transacao.valor else "",
                transacao.tipo or "",
                transacao.status_financeiro or "",
                transacao.status_aprovacao or "",
                transacao.data_emissao.strftime("%d/%m/%Y") if transacao.data_emissao else "",
                transacao.data_vencimento.strftime("%d/%m/%Y") if transacao.data_vencimento else "",
                transacao.data_pagamento,
                transacao.descricao or "",
                transacao.observacoes or "",
                transacao.contraparte or "",
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
