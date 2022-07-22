test_that("Interpretacao de janelas", {

    # Expansao de datahora

    faixa <- expandedatahora("2020")
    expect_equal(faixa, c("2020-01-01 00:00:00", "2021-01-01 00:00:00"))

    faixa <- expandedatahora("2020-04")
    expect_equal(faixa, c("2020-04-01 00:00:00", "2020-05-01 00:00:00"))

    faixa <- expandedatahora("2020-10-23")
    expect_equal(faixa, c("2020-10-23 00:00:00", "2020-10-24 00:00:00"))

    faixa <- expandedatahora("2020-06-05 11:00")
    expect_equal(faixa, c("2020-06-05 11:00:00", "2020-06-05 11:00:01"))

    # Montagem de condicionais

    cond <- parsedatas("2020", "data")
    expect_true(identical(cond, "data >= '2020-01-01 00:00:00' AND data < '2021-01-01 00:00:00'"))

    cond <- parsedatas("2019-03", "data")
    expect_true(identical(cond, "data >= '2019-03-01 00:00:00' AND data < '2019-04-01 00:00:00'"))

    cond <- parsedatas("2018-09-08", "data")
    expect_true(identical(cond, "data >= '2018-09-08 00:00:00' AND data < '2018-09-09 00:00:00'"))

    cond <- parsedatas("2018-07-10 05:00", "data")
    expect_true(identical(cond, "data >= '2018-07-10 05:00:00' AND data < '2018-07-10 05:00:01'"))

    cond <- parsedatas("2018/2022", "data")
    expect_true(identical(cond, "data >= '2018-01-01 00:00:00' AND data < '2023-01-01 00:00:00'"))

    cond <- parsedatas("2020-08/2021", "data")
    expect_true(identical(cond, "data >= '2020-08-01 00:00:00' AND data < '2022-01-01 00:00:00'"))

    cond <- parsedatas("2017/2020-06", "data")
    expect_true(identical(cond, "data >= '2017-01-01 00:00:00' AND data < '2020-07-01 00:00:00'"))

    cond <- parsedatas("2020-01/2020-08", "data")
    expect_true(identical(cond, "data >= '2020-01-01 00:00:00' AND data < '2020-09-01 00:00:00'"))

    cond <- parsedatas("2020-01-20/2020-08", "data")
    expect_true(identical(cond, "data >= '2020-01-20 00:00:00' AND data < '2020-09-01 00:00:00'"))

    cond <- parsedatas("2017/2020-06-05", "data")
    expect_true(identical(cond, "data >= '2017-01-01 00:00:00' AND data < '2020-06-06 00:00:00'"))

    cond <- parsedatas("2020-01-01 10:00/2020-06-05 03:30", "data")
    expect_true(identical(cond, "data >= '2020-01-01 10:00:00' AND data < '2020-06-05 03:30:01'"))
})

conexoes <- list("LOCAL" = conectalocal(system.file("extdata/sempart", package = "dbrenovaveis")))

if(Sys.getenv("RUN_BANCO", FALSE)) {
    conexoes <- c(conexoes, list("BANCO" = conectabanco(Sys.getenv("TEST_USER"), Sys.getenv("TEST_DB"))))
}

for(tipo in names(conexoes)) {

conn <- conexoes[[tipo]]

test_that(paste0(tipo, ": Interpretacao de arugmentos - USINAS"), {

    expect_error(parseargs_usinas(conn))

    arg <- parseargs_usinas(conn, "baebau")
    expect_equal(attr(arg, "n"), 1)
    expect_equal(arg[1], "id_usina IN (1)")

    arg <- parseargs_usinas(conn, c("baebau", "sceamo"))
    expect_equal(attr(arg, "n"), 2)
    expect_equal(arg[1], "id_usina IN (1, 14)")

    arg <- parseargs_usinas(conn, "*")
    expect_equal(attr(arg, "n"), 0)
    expect_true(is.na(arg[1]))

    arg2 <- parseargs_usinas(conn, NA)
    expect_true(identical(arg, arg2))
})

test_that(paste0(tipo, ": Interpretacao de arugmentos - DATAHORAS"), {

    expect_error(parseargs_datahoras(conn))

    arg <- parseargs_datahoras("2021-01-01", "teste")
    expect_equal(arg[1], "teste >= '2021-01-01 00:00:00' AND teste < '2021-01-02 00:00:00'")

    arg <- parseargs_datahoras(NA, "teste")
    expect_equal(arg[1], "teste >= '0001-01-01 00:00:00' AND teste < '4000-01-01 00:00:00'")

    arg2 <- parseargs_datahoras("*", "teste")
    expect_true(identical(arg, arg2))
})

test_that(paste0(tipo, ": Interpretacao de arugmentos - MODELOS"), {

    expect_error(parseargs_modelos(conn))

    arg <- parseargs_modelos(conn, "gfs")
    expect_equal(attr(arg, "n"), 1)
    expect_equal(arg[1], "id_modelo IN (1)")

    arg <- parseargs_modelos(conn, c("gfs", "ECMWF"))
    expect_equal(attr(arg, "n"), 2)
    expect_equal(arg[1], "id_modelo IN (1, 2)")

    arg <- parseargs_modelos(conn, "*")
    expect_equal(attr(arg, "n"), 0)
    expect_true(is.na(arg[1]))

    arg2 <- parseargs_modelos(conn, NA)
    expect_true(identical(arg, arg2))
})

test_that(paste0(tipo, ": Interpretacao de arugmentos - HORIZONTES"), {

    expect_error(parseargs_horizontes(conn))

    arg <- parseargs_horizontes(conn, 1)
    expect_equal(attr(arg, "n"), 1)
    expect_equal(arg[1], "dia_previsao IN (1)")

    arg <- parseargs_horizontes(conn, seq(2))
    expect_equal(attr(arg, "n"), 2)
    expect_equal(arg[1], "dia_previsao IN (1, 2)")

    arg2 <- parseargs_horizontes(conn, c(1, "D2"))
    expect_true(identical(arg, arg2))

    arg <- parseargs_horizontes(conn, "*")
    expect_equal(attr(arg, "n"), 0)
    expect_true(is.na(arg[1]))

    arg2 <- parseargs_horizontes(conn, NA)
    expect_true(identical(arg, arg2))
})

test_that(paste0(tipo, ": Interpretacao de argumentos - CAMPOS/ORDERBY"), {

    expect_error(parseargs(conn))

    # Verificados --------------------------------------------------------

    query <- parseargs(conn, "verificados", "baebau")
    expect_equal(query$SELECT, "data_hora,vento,geracao")
    expect_equal(query[["ORDER BY"]], "data_hora")

    query <- parseargs(conn, "verificados", "baebau", datahoras = "2021-01-01")
    expect_equal(query$SELECT, "data_hora,vento,geracao")
    expect_equal(query[["ORDER BY"]], "data_hora")

    query <- parseargs(conn, "verificados", c("baebau", "rnuem3"), datahoras = "2021-01-01")
    expect_equal(query$SELECT, "data_hora,vento,geracao,id_usina")
    expect_equal(query[["ORDER BY"]], "id_usina,data_hora")

    query <- parseargs(conn, "verificados", "baebau", datahoras = "2021-01-01", campos = "vento")
    expect_equal(query$SELECT, "data_hora,vento")
    expect_equal(query[["ORDER BY"]], "data_hora")

    query <- parseargs(conn, "verificados", c("baebau", "rnuem3"), datahoras = "2021-01-01",
        campos = "vento")
    expect_equal(query$SELECT, "data_hora,vento,id_usina")
    expect_equal(query[["ORDER BY"]], "id_usina,data_hora")

    # Previstos ----------------------------------------------------------

    # Selecionando usinas

    query <- parseargs(conn, "previstos", "baebau")
    expect_equal(query$SELECT, "data_hora_previsao,dia_previsao,vento,id_modelo")
    expect_equal(query[["ORDER BY"]], "id_modelo,dia_previsao,data_hora_previsao")

    query <- parseargs(conn, "previstos", "baebau", datahoras = "2021-01-01")
    expect_equal(query$SELECT, "data_hora_previsao,dia_previsao,vento,id_modelo")
    expect_equal(query[["ORDER BY"]], "id_modelo,dia_previsao,data_hora_previsao")

    query <- parseargs(conn, "previstos", c("baebau", "sceamo"), datahoras = "2021-01-01")
    expect_equal(query$SELECT, "data_hora_previsao,dia_previsao,vento,id_usina,id_modelo")
    expect_equal(query[["ORDER BY"]], "id_usina,id_modelo,dia_previsao,data_hora_previsao")

    # Selecionando modelos

    query <- parseargs(conn, "previstos", "baebau", datahoras = "2021-01-01", modelos = "gfs")
    expect_equal(query$SELECT, "data_hora_previsao,dia_previsao,vento")
    expect_equal(query[["ORDER BY"]], "dia_previsao,data_hora_previsao")

    query <- parseargs(conn, "previstos", "baebau", datahoras = "2021-01-01", modelos = c("gfs", "ecmwf"))
    expect_equal(query$SELECT, "data_hora_previsao,dia_previsao,vento,id_modelo")
    expect_equal(query[["ORDER BY"]], "id_modelo,dia_previsao,data_hora_previsao")

    # Selecionando horizontes

    query <- parseargs(conn, "previstos", "baebau", datahoras = "2021-01-01", horizontes = 1)
    expect_equal(query$SELECT, "data_hora_previsao,vento,id_modelo")
    expect_equal(query[["ORDER BY"]], "id_modelo,data_hora_previsao")

    query <- parseargs(conn, "previstos", "baebau", datahoras = "2021-01-01", horizontes = seq(2))
    expect_equal(query$SELECT, "data_hora_previsao,dia_previsao,vento,id_modelo")
    expect_equal(query[["ORDER BY"]], "id_modelo,dia_previsao,data_hora_previsao")

    # Selecionando modelos e horizontes

    query <- parseargs(conn, "previstos", "baebau", datahoras = "2021-01-01", modelos = "gfs", horizontes = 1)
    expect_equal(query$SELECT, "data_hora_previsao,vento")
    expect_equal(query[["ORDER BY"]], "data_hora_previsao")

    query <- parseargs(conn, "previstos", "baebau", datahoras = "2021-01-01",
        modelos = c("gfs", "ecmwf"), horizontes = seq(3))
    expect_equal(query$SELECT, "data_hora_previsao,dia_previsao,vento,id_modelo")
    expect_equal(query[["ORDER BY"]], "id_modelo,dia_previsao,data_hora_previsao")
})

}