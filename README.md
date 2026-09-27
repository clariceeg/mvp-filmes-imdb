# MVP: Pipeline de filmes do IMDb

Pipeline de dados desenvolvido em PySpark no Databricks Free Edition para selecionar filmes lançados entre 2000 e 2026 e analisar a frequência dos gêneros entre os títulos selecionados.

#### Código: [notebook do projeto](./MVP_FILMES_IMDb.ipynb).

## Contexto de Negócios e Perguntas

### Objetivo

Uma loja de produtos culturais e entretenimento, que vende filmes, livros e jogos, planeja criar um espaço dedicado a filmes lançados entre 2000 e 2026. Para apoiar a escolha dos títulos, utilizei dados do IMDb e defini três faixas de lançamento: 2000 a 2009, 2010 a 2019 e 2020 a 2026. Em cada faixa, selecionei os 100 filmes com mais votos entre aqueles com nota média igual ou superior a 7,0 e gênero informado. A loja é um cenário hipotético criado para este trabalho acadêmico.

### As perguntas que orientaram o pipeline foram:

1. Quais são os até 100 filmes com mais votos no IMDb e nota média igual ou superior a 7,0 em cada faixa de lançamento?
  
2. Quais gêneros aparecem com maior frequência entre os filmes selecionados em cada faixa?

## Carga dos Dados

### Fonte e condições de uso

Usei os conjuntos IMDb Non-Commercial Datasets, disponibilizados para uso pessoal e não comercial. O projeto tem finalidade acadêmica; o cenário de uso pela loja não representa autorização para utilizar esses dados comercialmente. Uma aplicação comercial real exigiria uma licença apropriada. Os arquivos brutos não foram publicados neste repositório.

Information courtesy of IMDb (https://www.imdb.com). Used with permission.

### Condições de uso dos dados do IMDb.

| Arquivo | Estrutura e finalidade |
| --- | --- |
| `title.basics.tsv.gz` | Registro de títulos: `tconst`, `titleType`, `primaryTitle`, `originalTitle`, `isAdult`, `startYear`, `endYear`, `runtimeMinutes` e `genres`. |
| `title.ratings.tsv.gz` | Avaliações por título: `tconst`, `averageRating` e `numVotes`. |

O campo tconst identifica os títulos nos dois arquivos e permite relacioná-los. Os dados são fornecidos em arquivos TSV compactados. O código lê a primeira linha como cabeçalho e usa tabulação (\t) como separador.

### Armazenamento na nuvem

Baixei os dois arquivos da página de datasets do IMDb e os enviei ao volume /Volumes/mvp_filmes_imdb/default/imdb_raw/, no Databricks. O notebook lê os arquivos desse volume e executa as transformações no ambiente de nuvem. O volume conserva os dados de entrada em seu formato original.
Evidência a inserir: captura de tela do volume imdb_raw exibindo os dois arquivos. Salvar em evidencias/arquivos-brutos.png e substituir esta nota por ![Arquivos brutos no volume do Databricks](evidencias/arquivos-brutos.png).



## Modelagem e Catálogo de Dados

## Pipeline de Dados

## Qualidade de Dados

## Análise de Dados

## Autoavaliação
