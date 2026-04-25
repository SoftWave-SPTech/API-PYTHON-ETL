from sqlalchemy import select, text
from sqlalchemy.orm import Session
import logging

from app.db.models import Transacao
from app.schemas import Banco, LinhaConciliacao, ResultadoEtl, TransacaoNormalizada

logger = logging.getLogger(__name__)


SQL_CREATE_TRANSACOES = """
CREATE TABLE IF NOT EXISTS transacao (
    id INT AUTO_INCREMENT PRIMARY KEY,
    usuario_id VARCHAR(64),
    honorario_id INT,
    titulo VARCHAR(150),
    valor DECIMAL(10,2),
    tipo VARCHAR(50),
    status_financeiro VARCHAR(50),
    status_aprovacao VARCHAR(50),
    data_emissao DATE,
    data_vencimento DATE,
    data_pagamento VARCHAR(10) NOT NULL,
    descricao TEXT,
    observacoes TEXT,
    contraparte VARCHAR(150),
    arquivo_origem VARCHAR(255) NOT NULL,
    data_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def garantir_tabela_transacoes(session: Session) -> None:
    """Cria as tabelas se não existirem"""
    # Cria a tabela transacao com a foreign key
    session.execute(text(SQL_CREATE_TRANSACOES))


def _garantir_coluna_usuario_id(session: Session) -> None:
    """Compatibilidade com bancos já existentes sem a coluna usuario_id."""
    rows = session.execute(text("SHOW COLUMNS FROM transacao LIKE 'usuario_id'")).fetchall()
    if not rows:
        session.execute(text("ALTER TABLE transacao ADD COLUMN usuario_id VARCHAR(64) NULL"))


def _variantes_data(data: str) -> list[str]:
    """YYYY-MM-DD e DD/MM/AAAA para conciliar registros novos e legados."""
    data = (data or "").strip()
    if not data:
        return [data]
    partes_barra = data.split("/")
    if len(partes_barra) == 3:
        d, m, y = partes_barra
        iso = f"{y}-{m}-{d}"
        return list(dict.fromkeys([data, iso]))
    partes_hifen = data.split("-")
    if len(partes_hifen) == 3:
        y, m, d = partes_hifen
        br = f"{d}/{m}/{y}"
        return list(dict.fromkeys([data, br]))
    return [data]


def transacao_existente_id(session: Session, t: TransacaoNormalizada, usuario_id: str | None) -> int | None:
    """
    Conciliação: mesma data, mesmo tipo (receita/despesa) e mesmo valor.
    """
    datas = _variantes_data(t.data_pagamento)
    q = (
        select(Transacao.id)
        .where(
            Transacao.usuario_id == usuario_id,
            Transacao.data_pagamento.in_(datas),
            Transacao.tipo == t.tipo.value,
            Transacao.valor == t.valor,
        )
        .limit(1)
    )
    return session.execute(q).scalar_one_or_none()


def inserir_transacao(session: Session, t: TransacaoNormalizada, arquivo_origem: str, usuario_id: str | None) -> int:
    """Insere uma linha na tabela transacao (após checagem de duplicidade por data/tipo/valor)."""
    nova = Transacao(
        usuario_id=usuario_id,
        honorario_id=None,
        data_pagamento=t.data_pagamento[:10],
        descricao=(t.descricao or "")[:255],
        tipo=t.tipo.value,
        valor=t.valor,
        arquivo_origem=arquivo_origem[:255],
    )
    session.add(nova)
    session.flush()
    return int(nova.id)


def processar_com_conciliacao(
    session: Session,
    banco: Banco,
    arquivo_origem: str,
    itens: list[TransacaoNormalizada],
    persistir: bool,
    usuario_id: str | None = None,
) -> ResultadoEtl:
    linhas: list[LinhaConciliacao] = []
    duplicatas = 0
    inseridas = 0

    if persistir:
        try:
            garantir_tabela_transacoes(session)
            _garantir_coluna_usuario_id(session)
        except Exception as e:
            logger.error(f"Erro ao criar tabela transacoes: {e}")
            raise

    for t in itens:
        ja_existia = False
        inserida = False
        transacao_id: int | None = None
        if persistir:
            try:
                existente_id = transacao_existente_id(session, t, usuario_id)
                ja_existia = existente_id is not None
                if not ja_existia:
                    transacao_id = inserir_transacao(session, t, arquivo_origem, usuario_id)
                    inserida = True
                    inseridas += 1
                else:
                    transacao_id = existente_id
                    duplicatas += 1
            except Exception as e:
                logger.error(f"Erro ao processar transacao {t}: {e}")
                raise
        linhas.append(
            LinhaConciliacao(transacao=t, ja_existia=ja_existia, inserida=inserida, transacao_id=transacao_id)
        )

    if persistir:
        try:
            session.commit()
            logger.info(f"✓ Conciliacao persistida: {inseridas} inseridas, {duplicatas} duplicatas")
        except Exception as e:
            session.rollback()
            logger.error(f"✗ Erro ao fazer commit da conciliacao: {e}")
            raise

    return ResultadoEtl(
        banco=banco,
        arquivo_origem=arquivo_origem,
        total_extraido=len(itens),
        duplicatas_ignoradas=duplicatas,
        inseridas=inseridas,
        linhas=linhas,
    )
