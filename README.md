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

Usei os conjuntos IMDb Non-Commercial Datasets em https://www.imdb.com/interfaces/, disponibilizados para uso pessoal e não comercial. O projeto tem finalidade acadêmica; o cenário de uso pela loja não representa autorização para utilizar esses dados comercialmente. Uma aplicação comercial real exigiria uma licença apropriada. Os arquivos brutos não foram publicados neste repositório.

Information courtesy of IMDb (https://www.imdb.com). Used with permission.

### Arquivos de origem

| Arquivo | Estrutura e finalidade |
| --- | --- |
| `title.basics.tsv.gz` | Registro de títulos: `tconst`, `titleType`, `primaryTitle`, `originalTitle`, `isAdult`, `startYear`, `endYear`, `runtimeMinutes` e `genres`. |
| `title.ratings.tsv.gz` | Avaliações por título: `tconst`, `averageRating` e `numVotes`. |

O campo tconst identifica os títulos nos dois arquivos e permite relacioná-los. Os dados são fornecidos em arquivos TSV compactados. O código lê a primeira linha como cabeçalho e usa tabulação (\t) como separador.


### Armazenamento na nuvem

Baixei os dois arquivos da página de datasets do IMDb e os enviei ao volume /Volumes/mvp_filmes_imdb/default/imdb_raw/, no Databricks. O notebook lê os arquivos desse volume e executa as transformações no ambiente de nuvem. O volume conserva os dados de entrada em seu formato original.
<img width="1051" height="231" alt="image" src="https://github.com/user-attachments/assets/caa25cdc-2dbc-4cda-b240-b80f664d137b" />


## Modelagem e Catálogo de Dados

Usei um modelo de duas tabelas finais, adequado às duas perguntas do projeto. catalogo_filmes contém um registro por filme selecionado; frequencia_generos contém uma linha por combinação de período e gênero. Os dois conjuntos derivam da junção dos arquivos brutos pelo identificador tconst. O primeiro permite consultar os títulos; o segundo, comparar os gêneros dos títulos escolhidos. As tabelas ficam no catálogo mvp_filmes_imdb, esquema default, e são gravadas pelo notebook.

### mvp_filmes_imdb.default.catalogo_filmes
| Campo | Tipo salvo | Significado, valores e origem |
| --- | --- | --- |
| `primaryTitle` | texto (`string`) | Nome principal do filme; vem de `title.basics`. A seleção final foi verificada quanto a títulos repetidos. |
| `startYear` | texto (`string`) | Ano de lançamento vindo de `title.basics`; convertido para inteiro nas condições de filtragem. Faixa selecionada: 2000 a 2026. |
| `genres` | texto (`string`) | Um ou mais gêneros do IMDb, separados por vírgula; valores ausentes representados por `\N` foram descartados. |
| `averageRating` | texto (`string`) | Nota média vinda de `title.ratings`; convertida para número decimal na condição de filtragem. Mínimo selecionado: 7,0. |
| `numVotesNumeric` | inteiro longo (`long`) | Número de votos, criado a partir de `numVotes` por conversão para `BIGINT`; usado para ordenar cada período. |

<img width="1340" height="599" alt="image" src="https://github.com/user-attachments/assets/e55a47e3-447a-433e-8063-0aa9b7eb4c8e" />


### mvp_filmes_imdb.default.frequencia_generos
| Campo | Tipo salvo | Significado, valores e origem |
| --- | --- | --- |
| `genre` | texto (`string`) | Gênero individual obtido da divisão de `genres` por vírgula e expansão da lista. |
| `count` | inteiro longo (`bigint`) | Quantidade de filmes selecionados associados ao gênero no período; é uma frequência, não a soma de votos. |
| `periodo` | texto (`string`) | Faixa de lançamento: `2000-2009`, `2010-2019` ou `2020-2026`. |

<img width="1355" height="601" alt="image" src="https://github.com/user-attachments/assets/7e39b42d-7e0a-4214-88be-9e2ee4fe133f" />


Na tabela de frequência, um filme classificado em três gêneros contribui uma vez para cada um. Por isso, a soma das frequências de um período pode ultrapassar 100. Para as análises futuras, seria útil manter tconst na tabela de filmes como identificador estável; neste MVP, ele foi retirado da apresentação final após a verificação de títulos repetidos na seleção.


## Pipeline de Dados

Implementei o fluxo em um único [notebook PySpark](./MVP_FILMES_IMDb.ipynb), com estas etapas:

1. Extração: leitura de title.basics.tsv.gz e title.ratings.tsv.gz do volume do Databricks como TSV com cabeçalho.
2. Integração: junção interna dos arquivos por tconst, associando atributos do filme à nota e ao número de votos.
3. Tratamento: manutenção de titleType = 'movie', exclusão de genres = '\N', filtragem de anos entre 2000 e 2026 após conversão para inteiro e de notas a partir de 7,0 após conversão para decimal. O número de votos é convertido para BIGINT em numVotesNumeric.
4. Seleção: divisão nos períodos 2000–2009, 2010–2019 e 2020–2026; ordenação decrescente de numVotesNumeric e limite de 100 filmes para cada período; união dos três conjuntos.
5. Agregação: separação da lista de gêneros, criação de um registro por filme e gênero e contagem por gênero em cada período.
6. Carga: gravação das tabelas catalogo_filmes e frequencia_generos com saveAsTable e modo overwrite. A leitura posterior das tabelas confirma sua disponibilidade para consulta no ambiente.

Os arquivos no volume representam a entrada bruta; os DataFrames do notebook reúnem os dados tratados; as duas tabelas gravadas são as saídas prontas para as consultas do MVP. A gravação em modo overwrite permite reconstruir as saídas ao executar novamente o notebook, usando a versão dos arquivos de entrada disponível naquele momento.

## Qualidade de Dados

Usei regras de filtragem alinhadas ao objetivo para impedir que registros sem gênero declarado ou fora dos critérios de ano e nota aparecessem na seleção. A conversão try_cast(startYear AS INT) evita tratar texto inválido como ano; try_cast(averageRating AS DOUBLE) faz o mesmo para a nota; try_cast(numVotes AS BIGINT) cria um campo numérico para a ordenação. Valores que não puderem ser convertidos para ano ou nota não atendem aos respectivos filtros.

Conferi a quantidade final de 300 filmes e agrupei primaryTitle para procurar nomes repetidos; nessa seleção, a contagem de títulos com mais de um registro foi 0. Também foram apresentados os resultados por período. Essa verificação de nome repetido não equivale a uma avaliação completa de duplicidade pelo identificador tconst.
<img width="1347" height="703" alt="image" src="https://github.com/user-attachments/assets/d688437f-4ec6-4039-85dd-214cf79f2551" />

Limitação da verificação: o notebook ainda não registra uma medição sistemática da proporção de nulos e valores vazios em cada coluna bruta, nem um levantamento de valores extremos. Portanto, não afirmo que todos os atributos de origem estejam completos ou sem outliers.

## Análise de Dados

### Filmes selecionados
A primeira pergunta foi respondida pela tabela catalogo_filmes: selecionei os 100 filmes com mais votos em cada período, desde que tivessem nota média igual ou superior a 7,0 e gênero declarado. Assim, o catálogo reúne 300 filmes. A ordenação pelo número de votos prioriza títulos com maior participação nas avaliações do IMDb; ela não mede vendas, preferência dos clientes da loja ou qualidade objetiva das obras.
<img width="1358" height="797" alt="image" src="https://github.com/user-attachments/assets/2eb60ad3-a601-48e7-bb32-913840c7679e" />

### Frequência dos gêneros
A segunda pergunta foi respondida pela tabela frequencia_generos. Entre os 100 filmes escolhidos em cada período, os gêneros mais frequentes foram:
| Período | Gênero mais frequente | Filmes associados ao gênero |
| --- | --- | ---: |
| 2000–2009 | Drama | 45 |
| 2010–2019 | Action (Ação) | 44 |
| 2020–2026 | Drama | 49 |

Drama liderou em dois dos três períodos analisados, enquanto Action liderou de 2010 a 2019. A contagem corresponde à presença do gênero entre os títulos selecionados, e não ao total de votos recebidos pelos filmes daquele gênero. Como os filmes podem ter mais de um gênero, os totais das categorias não formam uma divisão exclusiva dos 100 filmes. O recorte 2020–2026 contém dados disponíveis até a data da coleta e pode mudar com novas atualizações do IMDb.

<img width="328" height="438" alt="image" src="https://github.com/user-attachments/assets/8cb17c0e-7248-423d-8d27-c9b925befa40" /> <img width="329" height="373" alt="image" src="https://github.com/user-attachments/assets/6e1eff72-5a33-425a-98a2-75b7fd02f40d" /> <img width="333" height="371" alt="image" src="https://github.com/user-attachments/assets/3482d97c-8ee8-40dd-b653-475f51c11b8f" />

## Autoavaliação

Consegui realizar o fluxo principal planejado: coletar dois arquivos do IMDb, armazená-los na nuvem, combinar e filtrar os registros, selecionar filmes por período, contar gêneros e persistir as duas tabelas finais. As consultas retornaram 300 filmes, distribuídos em 100 por período. Trabalhar com tipos inicialmente lidos como texto e reconstruir as variáveis do notebook após mudanças de sessão foram dificuldades encontradas durante a implementação; resolvi esses pontos com conversões explícitas e uma sequência de células que pode ser executada desde o início.
Reconheço como limite a análise de qualidade ainda incompleta para todos os campos dos arquivos de origem. Também não analisei vendas ou preferências de clientes, pois esses dados não fazem parte dos conjuntos utilizados. Como evolução, eu incluiria verificações automatizadas de completude e unicidade por tconst, armazenaria tipos numéricos definitivos nas tabelas finais, documentaria a data de atualização dos arquivos e compararia a seleção com dados próprios da loja, caso estivessem disponíveis e licenciados para esse fim.
