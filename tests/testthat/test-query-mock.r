
test_that("Testes de modificacao de query", {

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

    parsed_2 <- query2subset(parseargs(tabela1, c("data", "valor"), data = "2000/"))
    expect_equal(parsed_2$SELECT, c("data", "valor"))
    expect_equal(parsed_2$FROM, "tabela_teste")
    expect_true(is.list(parsed_2$WHERE))
    expect_equal(names(parsed_2$WHERE), "data")
    expect_equal(parsed_2$WHERE$data, "(data >= '2000-01-01 00:00:00') & (data < '3999-01-01 00:00:01')")

    parsed_3 <- query2subset(parseargs(tabela1, c("codigo", "data", "valor"), codigo = c("A", "B")))
    expect_equal(parsed_3$SELECT, c("codigo", "data", "valor"))
    expect_equal(parsed_3$FROM, "tabela_teste")
    expect_true(is.list(parsed_3$WHERE))
    expect_equal(names(parsed_3$WHERE), "codigo")
    expect_equal(parsed_3$WHERE$codigo[1], "codigo %in% c('A','B')")
})

test_that("Leitura de dados mock", {

    arq  <- system.file("extdata/cpart_parquet/schema.json", package = "dbrenovaveis")
    conn <- conectamock(arq)

    # checagem de particionamento ----------------------------------------

    expect_true(!checa_particao(conn, list(FROM = "subbacias")))
    expect_true(checa_particao(conn, list(FROM = "assimilacao")))

    # leitura de tabela sem particao -------------------------------------

    query <- parseargs(conn$tabelas$subbacias, c("codigo", "nome", "bacia_smap"), codigo = "BAIXOIG")
    query <- query2subset(query)
    dat1  <- proc_query_mock_spart(conn, query)
    expect_snapshot_value(unlist(dat1), style = "deparse")

    # leitura de tabela com particao -------------------------------------

    query <- parseargs(conn$tabelas$previstos, c("data_previsao", "codigo", "rsolo"),
        dia_previsao = 1, data_previsao = "2020-01-01", codigo = "AVERMELHA")
    query <- query2subset(query)
    dat1  <- proc_query_mock_cpart(conn, query)
    expect_snapshot_value(unlist(dat1), style = "deparse")

})