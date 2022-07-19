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

if(Sys.getenv("RUNTEST")) {

test_that("Interpretacao de argumentos", {
    conn <- conectabanco(Sys.getenv("TEST_USER"), Sys.getenv("TEST_DB"))

    # Nao serao feitos testes a respeito da parte de datas, pois isso tem sua propria regiao

    expect_error(parseargs(conn))

    # execucoes limpas

    args_v0 <- parseargs(conn, "verificados", "*", "*", "*", "*", "*")
    expect_equal(args_v0$SELECT, "data_hora,vento,geracao,id_usina")
    expect_equal(args_v0$WHERE$usinas, "id_usina IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)")
    expect_equal(args_v0$WHERE$datahoras, "data_hora >= '0001-01-01 00:00:00' AND data_hora < '10000-01-01 00:00:00'")
    expect_equal(args_v0$WHERE$modelos, "id_modelo IN (1, 2)")
    expect_equal(args_v0$WHERE$horizontes, "dia_previsao IN (1, 2, 3, 4, 5)")

    args_p0 <- parseargs(conn, "previstos", "*", "*", "*", "*", "*")
    expect_equal(args_p0$SELECT, "data_hora_previsao,dia_previsao,vento,id_usina,id_modelo")
    expect_equal(args_p0$WHERE$usinas, args_v0$WHERE$usinas)
    expect_equal(args_p0$WHERE$datahoras,
        "data_hora_previsao >= '0001-01-01 00:00:00' AND data_hora_previsao < '10000-01-01 00:00:00'")
    expect_equal(args_p0$WHERE$modelos, args_v0$WHERE$modelos)
    expect_equal(args_p0$WHERE$horizontes, args_v0$WHERE$horizontes)

    # Execucoes com um parametro de cada vez

    args <- parseargs(conn, "verificados", c("baebau", "rnuem3"), "*", "*", "*", "*")
    expect_equal(args$SELECT, args_v0$SELECT)
    expect_equal(args$WHERE$usinas, "id_usina IN (1, 10)")
    expect_equal(args$WHERE$datahoras, args_v0$WHERE$datahoras)
    expect_equal(args$WHERE$modelos, args_v0$WHERE$modelos)
    expect_equal(args$WHERE$horizontes, args_v0$WHERE$horizontes)

    args <- parseargs(conn, "verificados", "*", datahoras = "2021-01-02", "*", "*", "*")
    expect_equal(args$SELECT, args_v0$SELECT)
    expect_equal(args$WHERE$usinas, args_v0$WHERE$usinas)
    expect_equal(args$WHERE$datahoras,
        "data_hora >= '2021-01-02 00:00:00' AND data_hora < '2021-01-03 00:00:00'")
    expect_equal(args$WHERE$modelos, args_v0$WHERE$modelos)
    expect_equal(args$WHERE$horizontes, args_v0$WHERE$horizontes)

    args <- parseargs(conn, "verificados", "*", "*", modelos = "ecmwf", "*", "*")
    expect_equal(args$SELECT, args_v0$SELECT)
    expect_equal(args$WHERE$usinas, args_v0$WHERE$usinas)
    expect_equal(args$WHERE$datahoras, args_v0$WHERE$datahoras)
    expect_equal(args$WHERE$modelos, "id_modelo IN (2)")
    expect_equal(args$WHERE$horizontes, args_v0$WHERE$horizontes)

    args <- parseargs(conn, "verificados", "*", "*", "*", horizontes = c("d2", "d5"), "*")
    expect_equal(args$SELECT, args_v0$SELECT)
    expect_equal(args$WHERE$usinas, args_v0$WHERE$usinas)
    expect_equal(args$WHERE$datahoras, args_v0$WHERE$datahoras)
    expect_equal(args$WHERE$modelos, args_v0$WHERE$modelos)
    expect_equal(args$WHERE$horizontes, "dia_previsao IN (2, 5)")

    # decisao de quais campos trazer em previstos

    args <- parseargs(conn, "previstos", "*", "*", modelos = "gfs", "*", "*")
    expect_equal(args$SELECT, "data_hora_previsao,dia_previsao,vento,id_usina")

    args <- parseargs(conn, "previstos", "*", "*", "*", horizontes = "d2", "*")
    expect_equal(args$SELECT, "data_hora_previsao,vento,id_usina,id_modelo")
})

}