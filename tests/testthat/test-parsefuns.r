
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

test_that("Parse de campos int, string e float", {

    # Campo INT ----------------------------------------------------------

    campo_int <- new_campo("campo_int", "int")

    expect_equal(parsearg(campo_int, "*"), structure(NA, "n" = 0))
    expect_equal(parsearg(campo_int, NA), structure(NA, "n" = 0))

    parsed_int <- parsearg(campo_int, c(1, 2, 4, 22))
    expect_equal(parsed_int[1], "campo_int IN (1,2,4,22)")
    expect_equal(attr(parsed_int, "valor"), c(1, 2, 4, 22))
    expect_equal(attr(parsed_int, "n"), 4)

    # Campo FLOAT --------------------------------------------------------

    campo_float <- new_campo("campo_float", "float")

    expect_equal(parsearg(campo_float, "*"), structure(NA, "n" = 0))
    expect_equal(parsearg(campo_float, NA), structure(NA, "n" = 0))

    parsed_float <- parsearg(campo_float, c(1, 2, 4, 22))
    expect_equal(parsed_float[1], "campo_float IN (1,2,4,22)")
    expect_equal(attr(parsed_float, "valor"), c(1, 2, 4, 22))
    expect_equal(attr(parsed_float, "n"), 4)

    # Campo STRING -------------------------------------------------------

    campo_string <- new_campo("campo_string", "string")

    expect_equal(parsearg(campo_string, "*"), structure(NA, "n" = 0))
    expect_equal(parsearg(campo_string, NA), structure(NA, "n" = 0))

    parsed_string <- parsearg(campo_string, c("a", "b", "c", "z"))
    expect_equal(parsed_string[1], "campo_string IN ('a','b','c','z')")
    expect_equal(attr(parsed_string, "valor"), c("a", "b", "c", "z"))
    expect_equal(attr(parsed_string, "n"), 4)
})

test_that("Parseargs completo", {

    # baseado em uma tabela fake
    tabela1 <- new_tabela(
        "tabela_teste",
        list(
            new_campo("codigo", "string"),
            new_campo("id", "int"),
            new_campo("data", "date"),
            new_campo("valor", "float")
        ),
        "/qualquer/caminho/local/",
        ".csv"
    )

    parsed_1 <- parseargs(tabela1, c("data", "valor"))
    expect_equal(parsed_1$SELECT, "data,valor")
    expect_equal(parsed_1$FROM, "tabela_teste")
    expect_true(is.null(parsed_1$WHERE))

    parsed_12 <- parseargs(tabela1)
    expect_equal(parsed_12$SELECT, "codigo,id,data,valor")
    expect_equal(parsed_12$FROM, "tabela_teste")
    expect_true(is.null(parsed_12$WHERE))

    parsed_2 <- parseargs(tabela1, c("data", "valor"), data = "2000/")
    expect_equal(parsed_2$SELECT, "data,valor")
    expect_equal(parsed_2$FROM, "tabela_teste")
    expect_true(is.list(parsed_2$WHERE))
    expect_equal(names(parsed_2$WHERE), "data")
    expect_equal(parsed_2$WHERE$data, "data >= '2000-01-01 00:00:00' AND data < '3999-01-01 00:00:01'")

    parsed_3 <- parseargs(tabela1, c("codigo", "data", "valor"), codigo = c("A", "B"))
    expect_equal(parsed_3$SELECT, "codigo,data,valor")
    expect_equal(parsed_3$FROM, "tabela_teste")
    expect_true(is.list(parsed_3$WHERE))
    expect_equal(names(parsed_3$WHERE), "codigo")
    expect_equal(parsed_3$WHERE$codigo[1], "codigo IN ('A','B')")
})