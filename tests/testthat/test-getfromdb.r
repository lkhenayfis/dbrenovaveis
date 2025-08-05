
# arrow fica emitindo um aviso que não dá pra controlar diretamente. mesmo desligando pelas opcoes
# do pacote ele continua saindo
w <- options()$warn
options(warn = -1)

test_that("Correcao de POSIX", {

    oldtz <- Sys.getenv("TZ")
    Sys.setenv("TZ" = "GMC")

    dat <- data.table(seq.POSIXt(as.POSIXct("2020-01-01 00:00:00"), length.out = 30, by = "30 min"))
    corrig <- corrigeposix(dat)

    Sys.setenv("TZ" = oldtz)

    expect_equal(attr(corrig$V1, "tzone"), "GMT")
    expect_equal(as.character(corrig$V1), as.character(dat$V1))
})

test_that("Leitura de banco mock", {

    arq  <- system.file("extdata/cpart_parquet/schema.json", package = "dbrenovaveis")
    conn <- conectamock(arq)

    dat1 <- getfromdb(conn, "subbacias", codigo = "AVERMELHA")
    expect_snapshot_value(unlist(dat1), style = "deparse")

    dat1 <- getfromdb(conn, "vazoes", data = "2020-01-01", codigo = "AVERMELHA")
    expect_snapshot_value(unlist(dat1), style = "deparse")
})

test_that("Leitura de banco S3 padrao", {
    arq <- "s3://ons-pem-historico/hidro/rodadas-smap/sintetico/schema.json"
    conn <- conectamock(arq)

    dat1 <- getfromdb(conn, "subbacias", codigo = "AVERMELHA")
    expect_snapshot_value(dat1, style = "serialize")

    dat2 <- getfromdb(conn, "precipitacao_observada", codigo = "AVERMELHA",
        data_previsao = "2020-01-01")
    expect_snapshot_value(dat2, style = "serialize")
})

test_that("Leitura de banco via morgana", {

    skip("morgana em manutencao")

    arq <- "s3://ons-pem-historico/hidro/rodadas-smap/sintetico/schema.json"
    conn <- conectamorgana(arq)

    dat1 <- getfromdb(conn, "subbacias", codigo = "AVERMELHA")
    expect_snapshot_value(dat1, style = "serialize")

    # a query via morgana retorna um erro de que nao existe a coluna 'codigo' na tabela
    # 'precipitacao_observada', embora exista e a mesma query, realizada pelo conector mock padrao
    # funcione adequadamente
    expect_error({
        dat2 <- getfromdb(conn, "precipitacao_observada", codigo = "AVERMELHA",
            data_previsao = "2020-01-01")
        expect_snapshot_value(dat2, style = "serialize")
    })
})

if (as.logical(Sys.getenv("TESTA_BANCO_POSTGRES", FALSE))) {

    test_that("Leitura de banco Postgres", {
        conn <- conectabanco("lucas", "banco_meta")
    })

}

options(warn = w)