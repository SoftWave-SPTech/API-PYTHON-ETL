from sqlalchemy import select, text
from sqlalchemy.orm import Session
import logging

from app.db.models import Transacao
from app.schemas import Banco, LinhaConciliacao, ResultadoEtl, TransacaoNormalizada

logger = logging.getLogger(__name__)


SQL_CREATE_TRANSACOES = """
CREATE TABLE IF NOT EXISTS transacao (
    id INT AUTO_INCREMENT PRIMARY KEY,
    usuario_id INT NOT NULL,
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
    data_insercao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_transacao_usuario FOREIGN KEY (usuario_id) REFERENCES usuario(id)
)
"""


def garantir_tabela_transacoes(session: Session) -> None:
    """Cria as tabelas se não existirem"""
    # Cria a tabela transacao com a foreign key
    session.execute(text(SQL_CREATE_TRANSACOES))


def _garantir_usuario_fk_obrigatorio(session: Session) -> None:
    """
    Garante que transacao.usuario_id seja INT NOT NULL e FK para usuario(id).
    """
    colunas = session.execute(text("SHOW COLUMNS FROM transacao LIKE 'usuario_id'")).mappings().all()
    if not colunas:
        session.execute(text("ALTER TABLE transacao ADD COLUMN usuario_id INT NULL"))
    session.execute(text("ALTER TABLE transacao MODIFY COLUMN usuario_id INT NOT NULL"))

    fk_rows = session.execute(
        text(
            """
            SELECT CONSTRAINT_NAME
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'transacao'
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
                ALTER TABLE transacao
                ADD CONSTRAINT fk_transacao_usuario
                FOREIGN KEY (usuario_id) REFERENCES usuario(id)
                """
            )
        )


def _validar_usuario_existe(session: Session, usuario_id: int) -> None:
    user_id = session.execute(
        text("SELECT id FROM usuario WHERE id = :usuario_id LIMIT 1"),
        {"usuario_id": usuario_id},
    ).scalar_one_or_none()
    if user_id is None:
        raise ValueError(f"Usuario {usuario_id} nao encontrado na tabela usuario.")


def garantir_estrutura_transacao(session: Session) -> None:
    garantir_tabela_transacoes(session)
    _garantir_usuario_fk_obrigatorio(session)


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


def transacao_existente_id(session: Session, t: TransacaoNormalizada, usuario_id: int) -> int | None:
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


def inserir_transacao(session: Session, t: TransacaoNormalizada, arquivo_origem: str, usuario_id: int) -> int:
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
    usuario_id: int,
) -> ResultadoEtl:
    linhas: list[LinhaConciliacao] = []
    duplicatas = 0
    inseridas = 0

    if persistir:
        try:
            garantir_tabela_transacoes(session)
            _garantir_usuario_fk_obrigatorio(session)
            _validar_usuario_existe(session, usuario_id)
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
            LinhaConciliacao(
                transacao=t,
                ja_existia=ja_existia,
                inserida=inserida,
                transacao_id=transacao_id,
                usuario_id=usuario_id if transacao_id is not None else None,
            )
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
