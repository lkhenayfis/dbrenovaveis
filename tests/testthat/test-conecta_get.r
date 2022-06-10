if(as.logical(Sys.getenv("RUNTEST"))) {

test_that("Testes de acesso ao banco", {

    conn <- conectabanco(Sys.getenv("TEST_USER"), Sys.getenv("TEST_DB"))
    expect_equal(class(conn)[1], "PostgreSQLConnection")

    verif <- getverificado(conn, "baebau", "2020-01-01", campos = c("vento", "geracao"))
    expect_equal(colnames(verif), c("data_hora", "vento", "geracao"))
    expect_equal(verif$data_hora,
        seq.POSIXt(
            as.POSIXct("2020-01-01 00:00:00 -03"),
            as.POSIXct("2020-01-01 23:30:00 -03"),
            by = "30 min")
    )
    expect_snapshot_value(round(verif$vento, 3), style = "deparse")
    expect_snapshot_value(round(verif$geracao, 3), style = "deparse")

    prev <- getprevisto(conn, "baebau", "2020-01-01", "GFS", "D1")
    expect_equal(colnames(prev), c("data_hora_previsao", "vento"))
    expect_equal(prev$data_hora,
        seq.POSIXt(
            as.POSIXct("2020-01-01 00:00:00 -03"),
            as.POSIXct("2020-01-01 23:30:00 -03"),
            by = "30 min")
    )
    expect_snapshot_value(round(prev$vento, 3), style = "deparse")
})

}