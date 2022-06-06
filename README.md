
<!-- README.md is generated from README.Rmd. Please edit that file -->

<!-- badges: start -->

[![R-CMD-check](https://github.com/lkhenayfis/dbrenovaveis/workflows/R-CMD-check/badge.svg)](https://github.com/lkhenayfis/dbrenovaveis/actions)
[![test-coverage](https://github.com/lkhenayfis/dbrenovaveis/workflows/test-coverage/badge.svg)](https://github.com/lkhenayfis/dbrenovaveis/actions)
<!-- badges: end -->

# dbrenovaveis

Este pacote contem funcoes para facilitacao do acesso aos bancos de
dados associados a area de energias renovaveis da PEM. Isto inclui
acesso, escrita e resgate de informacoes acerca dos bancos disponiveis.

## Instalacao

Este pacote ainda nao se encontra disponibilizado no CRAN, de modo que
deve ser instalado diretamente a partir do repositorio utilizando:

``` r
# Caso a biblioteca remotes nao esteja instalada, execute install.packages("remotes") primeiro
remotes::install_github("lkhenayfis/dbrenovaveis") # instalacao da versao de desenvolvimento
remotes::install_github("lkhenayfis/dbrenovaveis@*release") # instalacao da ultima versao fechada
```

## Exemplo de uso

Antes de utilizar as funcoes disponibilizadas pelo pacote, o usuario
deve primeiro registrar suas credenciais e parametros do banco a ser
acessado pelos metodos apropriados.

``` r
library(dbrenovaveis)

# registrando um banco chamado "dados", hospedado num servidor chamado "servidor-de-dados", 
# acessado pela porta 1111, tageado pelo mnemonico "banco_do_jose"
registra_banco(host = "servidor-de-dados", port = 1111, database = "dados", tag = "banco_do_jose")

# registrando um usuario "jose", cuja senha de acesso ao banco "dados" e "senha_do_jose"
registra_credenciais(usuario = "jose", senha = "senha_do_jose", tag = "usuario_jose")
```

A partir deste ponto, e possivel realizar a conexao com o banco e
extracao dos dados. A conexao e feita atraves da funcao `conectabanco`,
passando as `tag`s do usuario e banco registrados:

``` r
conn <- conectabanco("usuario_jose", "banco_do_jose")
```

Com a conexao feita, podemos utilizar as funcoes de extracao de dados. E
possivel acessar apenas os verificados…

``` r
verif <- getverificado(conn, "BAEBAU", "2021-01-01")
head(verif, 10)
#>              data_hora  vento
#> 1  2021-01-01 00:00:00  9.076
#> 2  2021-01-01 00:30:00 10.544
#> 3  2021-01-01 01:00:00 11.405
#> 4  2021-01-01 01:30:00 11.661
#> 5  2021-01-01 02:00:00 11.961
#> 6  2021-01-01 02:30:00 13.092
#> 7  2021-01-01 03:00:00 13.366
#> 8  2021-01-01 03:30:00 14.010
#> 9  2021-01-01 04:00:00 14.190
#> 10 2021-01-01 04:30:00 14.369
```

previstos …

``` r
prev <- getprevisto(conn, "BAEBAU", "2021-01-01", "D1")
head(prev, 10)
#>     data_hora_previsao vento_gfs vento_ecmwf
#> 1  2021-01-01 00:00:00     6.720       8.560
#> 2  2021-01-01 00:30:00     6.405       8.350
#> 3  2021-01-01 01:00:00     6.090       8.140
#> 4  2021-01-01 01:30:00     6.350       7.925
#> 5  2021-01-01 02:00:00     6.610       7.710
#> 6  2021-01-01 02:30:00     6.580       7.500
#> 7  2021-01-01 03:00:00     6.550       7.290
#> 8  2021-01-01 03:30:00     6.460       7.145
#> 9  2021-01-01 04:00:00     6.370       7.000
#> 10 2021-01-01 04:30:00     6.115       6.855
```

ou ambos combinados

``` r
dados <- getdados(conn, "BAEBAU", "2021-01-01", "D1")
head(dados, 10)
#>              data_hora  vento vento_gfs vento_ecmwf
#> 1  2021-01-01 00:00:00  9.076     6.720       8.560
#> 2  2021-01-01 00:30:00 10.544     6.405       8.350
#> 3  2021-01-01 01:00:00 11.405     6.090       8.140
#> 4  2021-01-01 01:30:00 11.661     6.350       7.925
#> 5  2021-01-01 02:00:00 11.961     6.610       7.710
#> 6  2021-01-01 02:30:00 13.092     6.580       7.500
#> 7  2021-01-01 03:00:00 13.366     6.550       7.290
#> 8  2021-01-01 03:30:00 14.010     6.460       7.145
#> 9  2021-01-01 04:00:00 14.190     6.370       7.000
#> 10 2021-01-01 04:30:00 14.369     6.115       6.855
```

Ate o presente momento nao foram implementadas as funcoes de upload ao
banco.
