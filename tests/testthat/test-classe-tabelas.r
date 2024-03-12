test_that("Criacao de objetos 'campo'", {

    campo1 <- new_campo("int_teste", "int")
    expect_equal(campo1$nome, "int_teste")
    expect_equal(class(campo1), c("campo_int", "campo"))

    campo2 <- new_campo("float_teste", "float")
    expect_equal(campo2$nome, "float_teste")
    expect_equal(class(campo2), c("campo_float", "campo"))

    expect_error(campo_erro <- new_campo("teste_erro", "tipo_qualquer"))
})

compfun1 <- function(x, ...) aws.s3::s3read_using(FUN = data.table::fread, object = x, ...)
compfun2 <- function(x, ...) aws.s3::s3read_using(FUN = arrow::read_parquet, object = x, ...)
compfun3 <- function(x, ...) aws.s3::s3read_using(FUN = jsonlite::read_json, object = x, ...)

test_that("Selecao de reader_fun", {

    # CSV ----------------------------------------------------------------

    ff <- switch_reader_func(".csv", FALSE)
    expect_equal(ff, data.table::fread)

    ff <- switch_reader_func("csv", FALSE)
    expect_equal(ff, data.table::fread)

    ff <- switch_reader_func(".csv", TRUE)
    expect_equal(formals(ff), formals(compfun1))
    expect_equal(body(ff), body(compfun1))

    ff <- switch_reader_func("csv", TRUE)
    expect_equal(formals(ff), formals(compfun1))
    expect_equal(body(ff), body(compfun1))

    # PARQUET ------------------------------------------------------------

    ff <- switch_reader_func(".parquet", FALSE)
    expect_equal(ff, arrow::read_parquet)

    ff <- switch_reader_func("parquet", FALSE)
    expect_equal(ff, arrow::read_parquet)

    ff <- switch_reader_func(".parquet", TRUE)
    expect_equal(formals(ff), formals(compfun2))
    expect_equal(body(ff), body(compfun2))

    ff <- switch_reader_func("parquet", TRUE)
    expect_equal(formals(ff), formals(compfun2))
    expect_equal(body(ff), body(compfun2))

    # JSON ---------------------------------------------------------------

    ff <- switch_reader_func(".json", FALSE)
    expect_equal(ff, jsonlite::read_json)

    ff <- switch_reader_func("json", FALSE)
    expect_equal(ff, jsonlite::read_json)

    ff <- switch_reader_func(".json", TRUE)
    expect_equal(formals(ff), formals(compfun3))
    expect_equal(body(ff), body(compfun3))

    ff <- switch_reader_func("json", TRUE)
    expect_equal(formals(ff), formals(compfun3))
    expect_equal(body(ff), body(compfun3))
})

test_that("Criacao de tabelas -- manual", {

    # TESTE DE TABELA LOCAL ----------------------------------------------

    tabela1 <- new_tabela(
        "tabela_teste",
        list(
            new_campo("codigo", "string"),
            new_campo("data", "date"),
            new_campo("valor", "float")
        ),
        "/qualquer/caminho/local/",
        ".csv"
    )

    expect_equal(class(tabela1), c("tabela_local", "tabela"))
    expect_equal(attr(tabela1, "uri"), "/qualquer/caminho/local/")
    expect_equal(attr(tabela1, "tipo_arquivo"), ".csv")
    expect_true(is.null(attr(tabela1, "descricao")))
    expect_equal(attr(tabela1, "reader_func"), data.table::fread)
    expect_equal(attr(tabela1, "master"), data.table::data.table(tabela = character(0)))
})

test_that("Criacao de tabelas -- dado teste s/ particao", {

    # TESTE DE TABELA LOCAL ----------------------------------------------

    dir <- system.file("extdata/cpart_parquet/subbacias/", package = "dbrenovaveis")
    arq <- file.path(dir, "schema.json")

    tabela1 <- schema2tabela(arq)

    expect_equal(class(tabela1), c("tabela_local", "tabela"))
    expect_equal(attr(tabela1, "uri"), dir)
    expect_equal(attr(tabela1, "tipo_arquivo"), ".parquet.gzip")
    expect_equal(attr(tabela1, "reader_func"), arrow::read_parquet)

    master <- attr(tabela1, "master")
    expect_equal(nrow(master), 1)
    expect_equal(colnames(master), "tabela")
})

test_that("Criacao de tabelas -- dado teste c/ particao", {

    # TESTE DE TABELA LOCAL ----------------------------------------------

    dir <- system.file("extdata/cpart_parquet/vazoes/", package = "dbrenovaveis")
    arq <- file.path(dir, "schema.json")

    tabela1 <- schema2tabela(arq)

    expect_equal(class(tabela1), c("tabela_local", "tabela"))
    expect_equal(attr(tabela1, "uri"), dir)
    expect_equal(attr(tabela1, "tipo_arquivo"), ".parquet.gzip")
    expect_equal(attr(tabela1, "reader_func"), arrow::read_parquet)

    master <- attr(tabela1, "master")
    expect_equal(nrow(master), 2)
    expect_equal(colnames(master), c("codigo", "tabela"))
    expect_equal(master$codigo, c("AVERMELHA", "BAIXOIG"))
})