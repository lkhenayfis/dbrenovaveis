
test_that("Testes de validacao de schema_tabela", {

    arq <- system.file("extdata/cpart_parquet/vazoes/schema.json", package = "dbrenovaveis")

    schema <- schema0 <- jsonlite::read_json(arq)

    schema$name <- NULL
    expect_error(valida_schema_tabela(schema))
    schema$name <- schema0$name

    schema$uri <- NULL
    expect_error(valida_schema_tabela(schema))
    schema$uri <- schema0$uri

    schema$uri <- "nome_errado"
    expect_error(valida_schema_tabela(schema))
    schema$uri <- schema0$uri

    schema$fileType <- "tipo_errado"
    expect_error(valida_schema_tabela(schema))
    schema$fileType <- schema0$fileType

    schema$fileType <- "parquet"
    expect_equal(valida_schema_tabela(schema)$fileType, ".parquet")
    schema$fileType <- schema0$fileType

    schema$columns[[1]]$name <- NULL
    expect_error(valida_schema_tabela(schema))
    schema$columns[[1]]$name <- schema0$columns[[1]]$name

    schema$columns[[1]]$type <- "tipo_errado"
    expect_error(valida_schema_tabela(schema))
    schema$columns[[1]]$type <- schema0$columns[[1]]$type
})

test_that("Testes de validacao de schema_banco", {

    arq <- system.file("extdata/cpart_parquet/schema.json", package = "dbrenovaveis")

    schema <- schema0 <- jsonlite::read_json(arq)

    # o schema do dado teste nao tem uri root e aponta para tabelas com uri relativas
    expect_error(valida_schema_banco(schema))

    schema$uri <- sub("/schema.json", "", arq)

    schema$tables <- NULL
    expect_error(valida_schema_banco(schema))
    schema$tables <- schema0$tables

    schema_valid <- valida_schema_banco(schema)
    expect_equal(length(schema_valid$tables), 6)
    expect_equal(sapply(schema_valid$tables, "[[", "name"),
        c("assimilacao", "parametros", "precipitacao_observada", "previstos", "vazoes", "subbacias"))
    expect_equal(sapply(schema_valid$tables, "[[", "uri"),
        file.path(sub("/schema.json", "", arq), ".",
            c("assimilacao", "parametros", "precipitacao_observada", "previstos", "vazoes", "subbacias"))
    )
})

test_that("Testes de composicao de schema completo", {

    # nao importa o conteudo, so pega o erro de passar os dois ao mesmo tempo
    expect_error(compoe_schema("teste", list()))

    arq <- system.file("extdata/cpart_parquet/schema.json", package = "dbrenovaveis")

    # lendo direto e passando lista, sem uri root, da erro
    schema_banco <- jsonlite::read_json(arq)
    expect_error(compoe_schema(schema_banco = schema_banco))

    schema <- compoe_schema(arq)

    root <- system.file("extdata/cpart_parquet", package = "dbrenovaveis")
    dirs <- file.path(root, sapply(schema$tables, "[[", "name"))

    schemas_tabelas <- lapply(file.path(dirs, "schema.json"), jsonlite::read_json)
    compara <- mapply(schemas_tabelas, schema$tables, FUN = function(tab1, tab2) {
        tab1$uri <- tab2$uri <- NULL
        identical(tab1, tab2)
    })
    expect_true(all(compara))
})