conexoes <- list("LOCAL" = conectalocal(system.file("extdata/sempart", package = "dbrenovaveis")))

if(Sys.getenv("RUN_BANCO", FALSE)) {
    conexoes <- c(conexoes, list("BANCO" = conectabanco(Sys.getenv("TEST_USER"), Sys.getenv("TEST_DB"))))
}

for(tipo in names(conexoes)) {

conn <- conexoes[[tipo]]
lf <- switch(tipo,
    "LOCAL" = dbrenovaveis:::listacampos.local,
    "BANCO" = dbrenovaveis:::listacampos.default
)

test_that(paste0(tipo, ": Leitura de tabelas qualitativas"), {

    # Tabela de usinas ----------------------------------------------

    datusi <- getusinas(conn)
    expect_equal(colnames(datusi), lf(conn, "usinas"))
    expect_equal(as.numeric(datusi[id == 1, "latitude"]), -9.89)
    expect_equal(as.numeric(datusi[id == 10, "longitude"]), -36.36)
    expect_equal(datusi[id == 14, data_inicio_operacao], as.POSIXct("2011-12-24", "GMT"))

    datusi1 <- getusinas(conn, "baebau")
    expect_equal(colnames(datusi), colnames(datusi1))
    expect_equal(nrow(datusi1), 1)
    expect_equal(datusi1[1, codigo], "BAEBAU")

    datusi2 <- getusinas(conn, c("baebau", "RNUEM3"))
    expect_equal(colnames(datusi), colnames(datusi2))
    expect_equal(nrow(datusi2), 2)
    expect_equal(datusi2[, codigo], c("BAEBAU", "RNUEM3"))

    # Tabela de modelos de previsao ---------------------------------

    datmodp <- getmodelos(conn)
    expect_equal(colnames(datmodp), lf(conn, "modelos_previsao"))
    expect_equal(datmodp[1, nome], "GFS")
    expect_equal(datmodp[1, horizonte_previsao], 5)

    datmodp1 <- getmodelos(conn, "gfs")
    expect_equal(colnames(datmodp1), colnames(datmodp))
    expect_equal(datmodp1[, nome], "GFS")
    expect_equal(datmodp1[, horizonte_previsao], 5)
})

}