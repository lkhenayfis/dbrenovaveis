
test_that("Leitura de banco mock", {

    arq  <- system.file("extdata/cpart_parquet/schema.json", package = "dbrenovaveis")
    conn <- conectamock(arq)

    dat1 <- getfromdb(conn, "subbacias", codigo = "AVERMELHA")
    expect_snapshot_value(unlist(dat1), style = "deparse")

    dat1 <- getfromdb(conn, "vazoes", data = "2020-01-01", codigo = "AVERMELHA")
    expect_snapshot_value(unlist(dat1), style = "deparse")
})

if (as.logical(Sys.getenv("TESTA_BANCO_POSTGRES", FALSE))) {

    test_that("Leitura de banco Postgres", {
        conn <- conectabanco("lucas", "banco_meta")
    })

}