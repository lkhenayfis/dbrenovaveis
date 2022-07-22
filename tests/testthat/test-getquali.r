if(as.logical(Sys.getenv("RUNTEST"))) {

test_that("Leitura de tabelas qualitativas", {
    conn <- conectabanco(Sys.getenv("TEST_USER"), Sys.getenv("TEST_DB"))
    expect_equal(class(conn)[1], "PostgreSQLConnection")

    # Tabela de usinas ----------------------------------------------

    datusi <- getusinas(conn)
    expect_equal(colnames(datusi), DBI::dbListFields(conn, "usinas"))
    expect_equal(as.numeric(datusi[7, "latitude"]), -7.73)
    expect_equal(as.numeric(datusi[12, "longitude"]), -50.33)
    expect_equal(datusi[3, data_inicio_operacao], as.POSIXct("2014-03-29", "GMT"))

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
    expect_equal(colnames(datmodp), DBI::dbListFields(conn, "modelos_previsao"))
    expect_equal(datmodp[1, nome], "GFS")
    expect_equal(datmodp[1, horizonte_previsao], 5)

    datmodp1 <- getmodelos(conn, "gfs")
    expect_equal(colnames(datmodp1), colnames(datmodp))
    expect_equal(datmodp1[, nome], "GFS")
    expect_equal(datmodp1[, horizonte_previsao], 5)
})

}