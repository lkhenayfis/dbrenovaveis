
<!-- README.md is generated from README.Rmd. Please edit that file -->

# dbrenovaveis

<!-- badges: start -->

[![R-CMD-check](https://github.com/lkhenayfis/dbrenovaveis/workflows/R-CMD-check/badge.svg)](https://github.com/lkhenayfis/dbrenovaveis/actions)
[![test-coverage](https://github.com/lkhenayfis/dbrenovaveis/workflows/test-coverage/badge.svg)](https://github.com/lkhenayfis/dbrenovaveis/actions)
[![codecov](https://codecov.io/gh/lkhenayfis/dbrenovaveis/graph/badge.svg?token=2E8YI878Y4)](https://codecov.io/gh/lkhenayfis/dbrenovaveis)
<!-- badges: end -->

## Visao geral

Este pacote fornece funcionalidade backend para interface com bancos
localizados em um diretorio local ou em um bucket S3. A versao atual
suporta particionamento de dados, porem apenas operacoes de leitura
estao implementadas. Criacao de novas tabelas e escrita nas existentes
serao adicionadas em versoes futuras.

## Instalacao

Este pacote ainda nao se encontra disponibilizado no CRAN, de modo que
deve ser instalado diretamente a partir do repositorio utilizando:

``` r
# Caso a biblioteca remotes nao esteja instalada, execute install.packages("remotes") primeiro
remotes::install_github("lkhenayfis/dbrenovaveis") # instalacao da versao de desenvolvimento
remotes::install_github("lkhenayfis/dbrenovaveis@*release") # instalacao da ultima versao fechada
```

## Exemplo de uso

Neste exemplo de uso sera utilizado o banco exemplo contido no pacote.
Para mais informacoes sobre como construir um banco proprio ou acessos a
dados em um bucket S3, consulte a vignette e paginas de documentacao
pertinentes.

O primeiro passo para acesso aos dados em um determinado banco e
estabeler uma conexao com o mesmo.

``` r
dir_banco_local <- system.file("extdata/cpart_parquet", package = "dbrenovaveis")
conexao <- conectamock(dir_banco_local)
```

Um `print` do objeto de conexao exibe as tabelas disponiveis, bem como
suas colunas

``` r
print(conexao)
#> * Banco 'mock' com tabelas: 
#> 
#> Tabela: assimilacao
#> - Conteudo: Dados de saida da assimilacao realizada com o smapR 
#> - Campos: codigo <string>, data <date>, dia_assimilacao <int>, qcalc <float>, rsolo <float>, rsup <float>, rsup2 <float>, rsub <float>, es <float>, er <float>, rec <float>, marg <float>, ed <float>, ed2 <float>, ed3 <float>, eb <float>, tu <float>, qsup1 <float>, qsup2 <float>, qplan <float>, qbase <float>, ebin <float>, supin <float>, peso_chuva <float> 
#> 
#> Tabela: parametros
#> - Conteudo: Parametros utilizados para execucao do smap 
#> - Campos: parametro <string>, valor <float>, codigo <string> 
#> 
#> Tabela: precipitacao_observada
#> - Conteudo: Dados de entrada de precipitacao observada 
#> - Campos: codigo <string>, data_previsao <date>, precipitacao <float> 
#> 
#> Tabela: previstos
#> - Conteudo: Dados de saida da previsao realizada com o smapR 
#> - Campos: codigo <string>, data_previsao <date>, dia_previsao <int>, qcalc <float>, rsolo <float>, rsup <float>, rsup2 <float>, rsub <float>, es <float>, er <float>, rec <float>, marg <float>, ed <float>, ed2 <float>, ed3 <float>, eb <float>, tu <float>, qsup1 <float>, qsup2 <float>, qplan <float>, qbase <float>, precipitacao <float> 
#> 
#> Tabela: vazoes
#> - Conteudo: Dados de entrada de vazao observada 
#> - Campos: data <date>, codigo <string>, vazao <float> 
#> 
#> Tabela: subbacias
#> - Conteudo:  
#> - Campos: posto <int>, nome <string>, codigo <string>, bacia_smap <string>, fator <float>, posto_jusante <int>, bacia <string>, posto_jusante_gvp_art <int>, posto_gvp_artificial <int>, tv <float>, n <int>, c1 <float>, c2 <float>, c3 <float>, latitude <float>, longitude <float>
```

A partir deste ponto e possivel acessar os dados do banco utilizando a
funcao `getfromdb`

``` r
# lendo tabela completa
dat <- getfromdb(conexao, tabela = "subbacias")
print(dat)
#>     posto          nome     codigo bacia_smap fator posto_jusante         bacia
#>  1:    18 Agua Vermelha  AVERMELHA   GRD_PRNB 1.000            34        Grande
#>  2:    24    Emborcacao EMBORCACAO   GRD_PRNB 1.000            31     Paranaiba
#>  3:    33     Sao Simao    SSIMAO2   GRD_PRNB 0.891            34     Paranaiba
#>  4:    61      Capivara   CAPIVARA  Paranazao 1.000            62  Paranapanema
#>  5:    81  Baixo Iguacu    BAIXOIG        SUL 0.204             0        Iguacu
#>  6:    92           Ita        ITA        SUL 1.000            94       Uruguai
#>  7:   111    Passo Real  PASSOREAL        SUL 1.000           112          Osul
#>  8:   156   Tres Marias     TMSMAP         NE 1.000           169 Sao Francisco
#>  9:   228       Colider    COLIDER      Norte 0.085           229   Teles Pires
#> 10:   237     B. Bonita    BBONITA  Paranazao 1.000           238         Tiete
#> 11:   266        Itaipu     ITAIPU  Paranazao 1.000             0        Parana
#> 12:   270 Serra da Mesa      SMESA      Norte 1.000           191     Tocantins
#> 13:   275       Tucurui    TUCURUI      Norte 1.000             0     Tocantins
#> 14:   278         Manso      MANSO        OSE 1.000             0      Paraguai
#> 15:   285         Jirau     JIRAU2      Norte 1.000           287       Madeira
#> 16:   288      Pimental  PIMENTALT      Norte 1.000             0         Xingu
#> 17:   290          Jari STOANTJARI      Norte 1.000             0          Jari
#>     posto_jusante_gvp_art posto_gvp_artificial     tv n c1 c2 c3   latitude
#>  1:                    34                   18  18.00 0  0  0  0 -19.867222
#>  2:                    31                   24  17.00 0  0  0  0 -18.451944
#>  3:                    34                   33  30.00 0  0  0  0 -19.018055
#>  4:                    62                   61   9.30 0  0  0  0 -22.657500
#>  5:                     0                   81   0.00 0  0  0  0 -25.503330
#>  6:                    94                   92  20.93 0  0  0  0 -27.267500
#>  7:                   112                  111   1.30 0  0  0  0 -29.016944
#>  8:                   169                  156 360.00 0  0  0  0 -18.212500
#>  9:                   229                  228 132.00 0  0  0  0 -10.984805
#> 10:                    38                   37  12.00 0  0  0  0 -22.505000
#> 11:                     0                   66   0.00 0  0  0  0 -25.426667
#> 12:                   191                  270  10.00 0  0  0  0 -13.826187
#> 13:                     0                  275   0.00 0  0  0  0  -3.750806
#> 14:                     0                  278   0.00 0  0  0  0 -14.875085
#> 15:                   287                  285  23.00 0  0  0  0  -9.266667
#> 16:                     0                  288   0.00 0  0  0  0  -3.438302
#> 17:                     0                  290   0.00 0  0  0  0  -0.650000
#>     longitude
#>  1: -50.35000
#>  2: -47.99389
#>  3: -50.49917
#>  4: -51.36083
#>  5: -53.67167
#>  6: -52.39833
#>  7: -53.18889
#>  8: -45.25917
#>  9: -55.76600
#> 10: -48.54500
#> 11: -54.59278
#> 12: -48.30083
#> 13: -49.66750
#> 14: -55.78838
#> 15: -64.65319
#> 16: -51.94512
#> 17: -52.51667
```

Subsets podem ser especificados no momento da leitura atraves de
argumentos opcionais nomeados homonimos das colunas na tabela

``` r
dat <- getfromdb(conexao, tabela = "subbacias", codigo = c("AVERMELHA", "BAIXOIG"))
print(dat)
#>    posto          nome    codigo bacia_smap fator posto_jusante  bacia
#> 1:    18 Agua Vermelha AVERMELHA   GRD_PRNB 1.000            34 Grande
#> 2:    81  Baixo Iguacu   BAIXOIG        SUL 0.204             0 Iguacu
#>    posto_jusante_gvp_art posto_gvp_artificial tv n c1 c2 c3  latitude longitude
#> 1:                    34                   18 18 0  0  0  0 -19.86722 -50.35000
#> 2:                     0                   81  0 0  0  0  0 -25.50333 -53.67167
```
