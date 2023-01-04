conexoes <- list(
    "LOCAL" = conectalocal(system.file("extdata/sempart", package = "dbrenovaveis")),
    "LOCAL_CP" = conectalocal(system.file("extdata/compart", package = "dbrenovaveis"))
)

if(Sys.getenv("RUN_BANCO", FALSE)) {
    conexoes <- c(conexoes, list("BANCO" = conectabanco(Sys.getenv("TEST_USER"), Sys.getenv("TEST_DB"))))
}

for(tipo in names(conexoes)) {

conn <- conexoes[[tipo]]

test_that(paste0(tipo, ": Testes de acesso ao banco"), {

    verif <- getverificado(conn, "BAEBAU", "2021-01-01", campos = c("vento", "geracao"))
    expect_equal(colnames(verif), c("data_hora", "vento", "geracao"))
    expect_equal(verif$data_hora,
        seq.POSIXt(
            as.POSIXct("2021-01-01 00:00:00", "GMT"),
            as.POSIXct("2021-01-01 23:30:00", "GMT"),
            by = "30 min")
    )
    expect_snapshot_value(round(verif$vento, 3), style = "deparse")
    expect_snapshot_value(round(verif$geracao, 3), style = "deparse")

    prev <- getprevisto(conn, "BAEBAU", "2021-01-01 00:00:00/2021-01-01 12:00:00", "GFS", "D1")
    expect_equal(colnames(prev), c("data_hora_previsao", "vento"))
    expect_equal(prev$data_hora,
        seq.POSIXt(
            as.POSIXct("2021-01-01 00:00:00", "GMT"),
            as.POSIXct("2021-01-01 12:00:00", "GMT"),
            by = "30 min")
    )
    expect_snapshot_value(round(prev$vento, 3), style = "deparse")

    if(tipo != "BANCO") {
        rean <- getreanalise(conn, longitudes = c(-50, -45), latitudes = c(5, 4))
        expect_equal(colnames(rean), c("id_vertice", "data_hora", "vento"))
        expect_equal(rean$id_vertice, rep(c(1, 345), each = 24))
        expect_equal(as.Date(rean$data_hora),
            rep(seq(as.Date("1979-01-01"), as.Date("1980-12-01"), by = "month"), 2))
    }
})

}