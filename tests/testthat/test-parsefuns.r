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
