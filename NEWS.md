# dbrenovaveis 0.3.2

## Bug fixes

* Corrige bug de query de usinas quando o codigo continha a string "IN". Dentro da adequacao para
  query em dados locais isto estava sendo substituido para "%in%" e retornando resultados errados

# dbrenovaveis 0.3.1

## Bug fixes

* Corrige problema no teste de `parsedatas` em sistemas operacionais linux
* Passa a exportar `conectalocal`

# dbrenovaveis 0.3

## New features

* Adiciona suporte ao pacote para leitura da tabela de ventos de reanalise
* Adiciona suporte a "archives" locais, isto e, um diretorio com estrutura similar a de um banco,
  porem cujas tabelas sao arquivos csv

## Bug fixes

* Corrige o erro nas queries em 2017 e 2018. Esse problema se dava na conversao de horarios de 
  virada de horario de verao, pois meia noite e meia noite e meia "nao existem". Foi introduzida
  uma funcao para execucao das queries que controla o tz da coluna de datas para evitar o erro.

# dbrenovaveis 0.2

## New features

* Adapta funcoes `getverificado` e `getprevisto` para o novo schema do banco
* `parsedatas` ganha argumento `query`, um booleano indicando se deve ser retornada a query ou
  apenas as datas expandidas
* Adiciona funcoes `getusinas` e `getmodelos` para pegar informacoes qualitativas a respeito das
  usinas e modelos de previsao meteorologica presentes no banco

# dbrenovaveis 0.1

Este pacote contem funcoes para facilitacao do acesso aos bancos de dados associados a area de
energias renovaveis da PEM. Isto inclui acesso, escrita e resgate de informacoes acerca dos bancos
disponiveis.