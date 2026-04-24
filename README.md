# API Python ETL

API FastAPI para extração, transformação e carregamento (ETL) de dados de extratos bancários de diferentes instituições financeiras brasileiras (C6, Bradesco e Itaú).

## 📋 Descrição

Esta API processa extratos bancários em formato CSV (C6 e Bradesco) e PDF (Itaú), normalizando os dados e permitindo armazená-los em um banco de dados. O sistema realiza:

- **Extração**: Leitura de múltiplos formatos de arquivo
- **Transformação**: Normalização de dados (datas, valores, descrições)
- **Conciliação**: Processamento e validação de transações
- **Persistência**: Armazenamento opcional em banco de dados

## 🚀 Como Executar

### Pré-requisitos

- Python 3.10 ou superior
- pip (gerenciador de pacotes Python)
- Git Bash (recomendado para executar os comandos)

### Instalação

1. **Clone o repositório:**
   ```bash
   git clone <URL_DO_REPOSITORIO>
   cd API-PYTHON-ETL
   ```

2. **Crie um ambiente virtual (opcional, mas recomendado):**
   ```bash
   python -m venv venv
   source venv/Scripts/activate
   ```

3. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure as variáveis de ambiente:**
   
   Crie um arquivo `.env` na raiz do projeto com as configurações do banco de dados:
   ```env
   DB_HOST=localhost
   DB_PORT=3306
   DB_USER=usuario
   DB_PASSWORD=senha
   DB_NAME=softwave
   DB_CONNECTION_TIMEOUT=15
   DB_AUTOCOMMIT=false
   DB_CHARSET=utf8mb4
   DB_USE_PURE=true
   API_TITLE=ETL Extratos Bancarios
    API_VERSION=1.0.0
   ```

5. **Execute a API (usando Git Bash):**
   ```bash
   py -m uvicorn app.main:app --reload
   ```

   A API estará disponível em: `http://localhost:8000`

## 📚 Endpoints

### Health Check

**GET** `/health`

Verifica se a API está funcionando.

**Resposta (200):**
```json
{
  "status": "ok"
}
```

### Upload de Extrato

**POST** `/etl/upload`

Faz upload de um extrato bancário e processa as transações.

**Parâmetros de Query:**
- `banco` (string, obrigatório): Tipo de banco (`c6`, `bradesco` ou `itau`)
- `persistir` (boolean, opcional, padrão: false): Se true, salva as transações no banco de dados

**Parâmetros de Arquivo:**
- `arquivo` (file, obrigatório): 
  - Para C6 e Bradesco: arquivo `.csv`
  - Para Itaú: arquivo `.pdf`

**Resposta (200):**
```json
{
  "banco": "c6",
  "arquivo": "extrato.csv",
  "total_processado": 25,
  "total_persistido": 24,
  "detalhes": [
    {
      "data_pagamento": "08/03/2026",
      "descricao": "Compra débito",
      "tipo": "despesa",
      "valor": "150.50"
    }
  ]
}
```

**Erros:**
- `400`: Arquivo vazio ou formato inválido
- `422`: Erro ao processar o arquivo (formato incorreto ou dados inválidos)

### Exportar Extrato em CSV

**GET** `/etl/extrato/csv`

Exporta todas as transações da tabela do banco de dados em formato CSV.

**Parâmetros:** Nenhum

**Resposta (200):**
Retorna um arquivo CSV com as seguintes colunas:
- `id`: ID da transação
- `honorario_id`: ID do honorário associado (chave estrangeira)
- `titulo`: Título da transação
- `valor`: Valor da transação
- `tipo`: Tipo de transação
- `status_financeiro`: Status financeiro
- `status_aprovacao`: Status de aprovação
- `data_emissao`: Data de emissão (DD/MM/YYYY)
- `data_vencimento`: Data de vencimento (DD/MM/YYYY)
- `data_pagamento`: Data de pagamento (DD/MM/YYYY)
- `descricao`: Descrição detalhada
- `observacoes`: Observações adicionais
- `contraparte`: Contraparte da transação
- `arquivo_origem`: Nome do arquivo de origem
- `data_insercao`: Data de inserção no banco (DD/MM/YYYY HH:MM:SS)

**Exemplo de conteúdo do CSV:**
```csv
id,honorario_id,titulo,valor,tipo,status_financeiro,status_aprovacao,data_emissao,data_vencimento,data_pagamento,descricao,observacoes,contraparte,arquivo_origem,data_insercao
1,10,Fatura 001,1500.00,receita,pago,aprovado,01/03/2026,15/03/2026,08/03/2026,Serviços prestados,Pago com sucesso,Empresa XYZ,extrato.csv,08/03/2026 10:30:45
```

**Erros:**
- `404`: Nenhuma transação encontrada na tabela
- `503`: Falha de conexão com banco de dados
- `500`: Erro ao exportar o extrato

## 🧪 Como Testar

### Opção 1: Swagger UI (Recomendado)

1. Acesse: `http://localhost:8000/docs`
2. Clique no endpoint `/etl/upload`
3. Preencha os parâmetros:
   - `banco`: Selecione uma opção (c6, bradesco, itau)
   - `persistir`: true ou false (opcional)
   - `arquivo`: Clique em "Choose File" e selecione o arquivo
4. Clique em "Execute"

#### Testar exportação de CSV

1. Acesse: `http://localhost:8000/docs`
2. Clique no endpoint `/etl/extrato/csv`
3. Clique em "Execute"
4. O navegador fará download do arquivo `extrato.csv`

### Opção 2: curl (Git Bash)

```bash
# C6 (CSV)
file="C:/caminho/para/extrato_c6.csv"
curl -X POST \
  -H "accept: application/json" \
  -F "banco=c6" \
  -F "arquivo=@$file" \
  http://localhost:8000/etl/upload

# Bradesco (CSV)
file="C:/caminho/para/extrato_bradesco.csv"
curl -X POST \
  -H "accept: application/json" \
  -F "banco=bradesco" \
  -F "arquivo=@$file" \
  http://localhost:8000/etl/upload

# Itaú (PDF)
file="C:/caminho/para/extrato_itau.pdf"
curl -X POST \
  -H "accept: application/json" \
  -F "banco=itau" \
  -F "arquivo=@$file" \
  http://localhost:8000/etl/upload

# Com persistência no banco de dados
file="C:/caminho/para/extrato.csv"
curl -X POST \
  -H "accept: application/json" \
  -F "banco=c6" \
  -F "persistir=true" \
  -F "arquivo=@$file" \
  http://localhost:8000/etl/upload

# Exportar extrato em CSV
curl -X GET \
  -H "accept: text/csv" \
  -o extrato_exportado.csv \
  http://localhost:8000/etl/extrato/csv
```

### Opção 3: Python/Requests

```python
import requests

url = "http://localhost:8000/etl/upload"
params = {
    "banco": "c6",
    "persistir": False
}

with open("extrato_c6.csv", "rb") as f:
    files = {"arquivo": f}
    response = requests.post(url, params=params, files=files)
    print(response.json())
```

### Opção 4: Insomnia/Postman

1. Crie uma nova requisição POST
2. URL: `http://localhost:8000/etl/upload`
3. Parâmetros (Query):
   - `banco`: c6 (ou bradesco/itau)
   - `persistir`: true (opcional)
4. Body → Form Data:
   - Key: `arquivo`, Value: Selecione o arquivo
5. Envie a requisição

## 📁 Estrutura do Projeto

```
API-PYTHON-ETL/
├── app/
│   ├── __init__.py
│   ├── main.py                 # Aplicação FastAPI principal
│   ├── config.py               # Configurações
│   ├── schemas.py              # Modelos Pydantic
│   ├── db/
│   │   ├── session.py          # Configuração do banco de dados
│   │   └── models.py           # Modelos SQLAlchemy
│   ├── etl/
│   │   ├── c6.py               # Processamento C6
│   │   ├── bradesco.py         # Processamento Bradesco
│   │   └── itau.py             # Processamento Itaú
│   ├── routers/
│   │   └── etl.py              # Definição dos endpoints
│   └── services/
│       └── conciliacao.py      # Lógica de conciliação
├── requirements.txt            # Dependências Python
└── README.md                   # Este arquivo
```

## 🔧 Dependências

- **FastAPI**: Framework web moderno
- **Uvicorn**: Servidor ASGI
- **SQLAlchemy**: ORM para banco de dados
- **Pydantic**: Validação de dados
- **Pandas**: Processamento de dados em CSV
- **pdfplumber**: Leitura de PDF
- **psycopg2**: Driver PostgreSQL
- **python-multipart**: Suporte a upload de arquivos
- **python-dotenv**: Carregamento de variáveis de ambiente

## 🏦 Formatos Suportados

### C6
- Formato: CSV
- Extensões: `.csv`
- Encoding: UTF-8, Latin1
- Separador: `,` ou `;`

### Bradesco
- Formato: CSV
- Extensões: `.csv`
- Encoding: UTF-8, Latin1
- Separador: `,` ou `;`

### Itaú
- Formato: PDF
- Extensões: `.pdf`

## 📅 Formato de Datas

Todas as datas são processadas e retornadas no formato: **DD/MM/YYYY**

Exemplo: `08/03/2026`

## 💰 Formato de Valores

Os valores são processados como números decimais com até 2 casas decimais.

Exemplo: `150.50`

## 🐛 Troubleshooting

### "Arquivo vazio"
- Certifique-se de que o arquivo selecionado não está vazio
- Verifique o caminho do arquivo

### "Arquivo com formato inválido"
- C6 e Bradesco: envie um arquivo `.csv`
- Itaú: envie um arquivo `.pdf`

### "Colunas obrigatórias ausentes"
- O arquivo precisa conter as colunas: data, descrição/histórico e valor
- Verifique o formato do arquivo enviado

### Erro de conexão com banco de dados
- Verifique se o `DATABASE_URL` está configurado corretamente no arquivo `.env`
- Certifique-se de que o banco de dados está em execução

## 📝 Notas

- O parâmetro `persistir=true` salva as transações no banco de dados
- Transações duplicadas são automaticamente removidas
- Descrições são limitadas a 255 caracteres
- Valores negativos são interpretados como despesas, positivos como receitas

## 📞 Suporte

Para mais informações ou dúvidas, consulte a documentação interativa em: `http://localhost:8000/docs`