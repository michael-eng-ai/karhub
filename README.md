# Pipeline ETL de Finanças Pessoais

## Visão Geral

Este projeto implementa um pipeline ETL (Extract, Transform, Load) para processar dados de finanças pessoais. O pipeline extrai dados de despesas e receitas de fontes específicas, transforma esses dados aplicando conversões de moeda e outras operações, e finalmente carrega os dados processados em um banco de dados MySQL e/ou Bigquery.

## Estrutura do Projeto
sao_paulo_budget_etl/
├── airflow/
│   ├── dags/
│   │   └── budget_etl_dag.py
├── src/
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── load-bq.py
│   └── utils.py
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── data/
│   ├── raw/
│   │   ├── gdvDespesasExcel.csv
│   │   └── gdvReceitasExcel.csv
│   └── processed/
├── config/
│   └── config.yaml
├── Dockerfile
├── requirements.txt
└── README.md

## Requisitos

- Python 3.7+
- Bibliotecas Python listadas em `requirements.txt`

## Instalação

1. Clone este repositório:


2. Crie um ambiente virtual (opcional, mas recomendado):
python -m venv venv
source venv/bin/activate # No Windows use venv\Scripts\activate

3. Instale as dependências:
pip install -r requirements.txt

## Configuração

Edite o arquivo `config.yaml` para configurar as fontes de dados, o caminho do banco de dados e outras opções:

```yaml
database:
path: /caminho/para/seu/banco.db
logging_level: INFO

4. Uso:
python run_tests.py

5. Para executar os testes unitários:
python run_tests.py

6. Extração (extract.py):
Extrai dados de despesas e receitas de fontes específicas.
Obtém dados de taxa de câmbio.

7. Transformação (transform.py):
Aplica conversões de moeda.
Realiza outras transformações necessárias nos dados.

8. Carregamento (load.py/load-bq.py):
Cria e gerencia a conexão com o banco de dados Mysql ou BQ.
Carrega os dados transformados no banco de dados.

9. Utilidades (utils.py):
Funções auxiliares para carregar configurações, configurar logging, etc.

10. Respostas:
Quais são as 5 fontes de recursos que mais arrecadaram?
| id_fonte_recurso | nome_fonte_recurso                               | total_arrecadado        |
|------------------|--------------------------------------------------|-------------------------|
| não-identificado | não-identificado                                  | 133.729.131.828.106,11  |
| 001              | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR            | 73.852.881.539.424,4    |
| 002              | RECURSOS VINCULADOS ESTADUAIS                     | 26.924.900.381.491,42   |
| 081              | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR-INTRA      | 15.866.540.653.989,78   |
| 004              | REC.PROPRIO-ADM.IND.-DOT.INIC.CR.SUPL.            | 5.450.395.453.986,06    |

Quais são as 5 fontes de recursos que mais gastaram?
| id_fonte_recurso | nome_fonte_recurso                               | total_liquidado          |
|------------------|--------------------------------------------------|--------------------------|
| 000              | não-identificado                                 | 134.016.901.337.412,56   |
| 001              | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR            | 69.337.611.542.067,85    |
| 002              | RECURSOS VINCULADOS ESTADUAIS                     | 26.361.683.907.972,16    |
| 081              | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR-INTRA      | 15.037.556.021.495,81    |
| 004              | REC.PROPRIO-ADM.IND.-DOT.INIC.CR.SUPL.            | 5.140.684.739.941,38     |

Quais são as 5 fontes de recursos com a melhor margem bruta?
| id_fonte_recurso | nome_fonte_recurso                               | margem_bruta             |
|------------------|--------------------------------------------------|--------------------------|
| não-identificado | não-identificado                                 | 133.729.131.828.106,11   |
| 001              | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR            | 4.515.269.997.356,55     |
| 081              | TESOURO-DOT.INICIAL E CRED.SUPLEMENTAR-INTRA      | 828.984.632.493,97       |
| 003              | RECURSOS VINCULADOS-FUNDO ESPECIAL DE DESPESAS    | 777.918.543.291,3        |
| 002              | RECURSOS VINCULADOS ESTADUAIS                     | 563.216.473.519,26       |

Quais são as 5 fontes de recursos que menir arrecadaram?
| id_fonte_recurso | nome_fonte_recurso                                       | total_arrecadado |
|------------------|----------------------------------------------------------|------------------|
| 000              | não-identificado                                         | 0                |
| 099              | EXTRA ORCAMENTARIA                                       | 0                |
| 043              | F.E.D - CREDITO POR SUPERAVIT FINANCEIRO                 | 0                |
| 041              | TESOURO - CREDITO POR SUPERAVIT FINANCEIRO               | 5,19             |
| 045              | REC.VINC.TRANSF.FEDERAL/SUPERAVIT FINANC.                | 67.855,75        |

Quais são as 5 fontes de recursos que menir gastaram?
| id_fonte_recurso | nome_fonte_recurso                                       | total_liquidado      |
|------------------|----------------------------------------------------------|----------------------|
| 099              | EXTRA ORCAMENTARIA                                       | 0                    |
| não-identificado | não-identificado                                         | 0                    |
| 084              | REC.PROPRIO-ADM.IND.-DOT.INIC.CR.SUPL.-INTRA             | 1.286.005.819,34     |
| 043              | F.E.D - CREDITO POR SUPERAVIT FINANCEIRO                 | 7.015.578.714,43     |
| 086              | OUTRAS FONTES DE RECURSOS-INTRA                          | 10.018.069.367,48    |

Quais são as 5 fontes de recursos com a pior margem bruta?
| id_fonte_recurso | nome_fonte_recurso                                          | margem_bruta           |
|------------------|-------------------------------------------------------------|------------------------|
| 000              | não-identificado                                            | -134.016.901.337.412,56 |
| 005              | RECURSOS VINCULADOS FEDERAIS                                | -1.329.798.014.124,57  |
| 041              | TESOURO - CREDITO POR SUPERAVIT FINANCEIRO                  | -549.329.076.656,96    |
| 007              | OP.CRED.E CONTRIB.DO EXTERIOR-DOT.INIC.CR.SU                | -475.580.960.278,29    |
| 047              | REC.OPERAC. DE CREDITO-P/SUPERAVIT FINANCEIR                | -438.792.482.756,9     |

Qual a média de arrecadação por fonte de recurso?
| media_arrecadacao                 |
|-----------------------------------|
| 11.418.656.743.019,723043         |

Qual a média de gastos por fonte de recurso?
| media_gastos                      |
|-----------------------------------|
| 11.251.810.359.508,528696         |