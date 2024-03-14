
test_that("Validadores de tipo de arquivo", {

    # testes padrao ------------------------------------------------------

    expect_equal(valida_tipo_arquivo("csv"), ".csv")
    expect_equal(valida_tipo_arquivo("parquet"), ".parquet")
    expect_equal(valida_tipo_arquivo("parquet.gzip"), ".parquet.gzip")
    expect_equal(valida_tipo_arquivo("json"), ".json")

    expect_equal(valida_tipo_arquivo("csv"), valida_tipo_arquivo(".csv"))
    expect_equal(valida_tipo_arquivo("parquet"), valida_tipo_arquivo(".parquet"))
    expect_equal(valida_tipo_arquivo("parquet.gzip"), valida_tipo_arquivo(".parquet.gzip"))
    expect_equal(valida_tipo_arquivo("json"), valida_tipo_arquivo(".json"))

    expect_error(valida_tipo_arquivo("tipo_qualquer"))
})

test_that("Selecao de reader_fun", {

    # CSV ----------------------------------------------------------------

    ff <- switch_reader_func(".csv", FALSE)
    expect_equal(ff, inner_csv)

    ff <- switch_reader_func("csv", FALSE)
    expect_equal(ff, inner_csv)

    ff <- switch_reader_func(".csv", TRUE)
    expect_equal(ff, outer_s3(inner_csv))

    ff <- switch_reader_func("csv", TRUE)
    expect_equal(ff, outer_s3(inner_csv))

    # PARQUET ------------------------------------------------------------

    ff <- switch_reader_func(".parquet", FALSE)
    expect_equal(ff, inner_parquet)

    ff <- switch_reader_func("parquet", FALSE)
    expect_equal(ff, inner_parquet)

    ff <- switch_reader_func(".parquet", TRUE)
    expect_equal(ff, outer_s3(inner_parquet))

    ff <- switch_reader_func("parquet", TRUE)
    expect_equal(ff, outer_s3(inner_parquet))

    # PARQUET.GZIP -------------------------------------------------------

    ff <- switch_reader_func(".parquet.gzip", FALSE)
    expect_equal(ff, inner_parquet_gzip)

    ff <- switch_reader_func("parquet.gzip", FALSE)
    expect_equal(ff, inner_parquet_gzip)

    ff <- switch_reader_func(".parquet.gzip", TRUE)
    expect_equal(ff, outer_s3(inner_parquet_gzip))

    ff <- switch_reader_func("parquet.gzip", TRUE)
    expect_equal(ff, outer_s3(inner_parquet_gzip))

    # JSON ---------------------------------------------------------------

    ff <- switch_reader_func(".json", FALSE)
    expect_equal(ff, inner_json)

    ff <- switch_reader_func("json", FALSE)
    expect_equal(ff, inner_json)

    ff <- switch_reader_func(".json", TRUE)
    expect_equal(ff, outer_s3(inner_json))

    ff <- switch_reader_func("json", TRUE)
    expect_equal(ff, outer_s3(inner_json))

    # ERROS --------------------------------------------------------------

    expect_error(ff <- switch_reader_func("tipo_qualquer", FALSE))
    expect_error(ff <- switch_reader_func("tipo_qualquer", TRUE))
})
