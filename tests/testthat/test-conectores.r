
test_that("Testa conexao mock -- Local", {

    arq  <- system.file("extdata/cpart_parquet/schema.json", package = "dbrenovaveis")
    conn <- conectamock(arq)
    expect_true(inherits(conn, "mock"))
    expect_equal(attr(conn, "uri"), sub("/schema.json", "", arq))

    arq2 <- system.file("extdata/cpart_parquet", package = "dbrenovaveis")
    conn2 <- conectamock(arq2)
    expect_identical(conn, conn2)

    schema <- compoe_schema(arq)
    conn3 <- conectamock(schema)
    expect_identical(conn, conn3)
})

test_that("Testa conexao mock -- S3", {

    arq  <- "s3://ons-pem-historico/hidro/rodadas-smap/sintetico/schema.json"
    conn <- conectamock(arq)
    expect_true(inherits(conn, "mock"))
    expect_equal(attr(conn, "uri"), sub("/schema.json", "", arq))

    arq2 <- "s3://ons-pem-historico/hidro/rodadas-smap/sintetico"
    conn2 <- conectamock(arq2)
    expect_identical(conn, conn2)

    schema <- compoe_schema(arq)
    conn3 <- conectamock(schema)
    expect_identical(conn, conn3)
})

test_that("Testa conexao mock -- morgana-client", {

    arq  <- "s3://ons-pem-historico/hidro/rodadas-smap/sintetico/schema.json"
    conn <- conectamorgana(arq)
    expect_true(inherits(conn, c("morgana", "mock")))
    expect_equal(attr(conn, "uri"), sub("/schema.json", "", arq))
    expect_equal(attr(conn, "x_api_key"), Sys.getenv("X_API_KEY"))

    arq2 <- "s3://ons-pem-historico/hidro/rodadas-smap/sintetico"
    conn2 <- conectamorgana(arq2)
    expect_identical(conn, conn2)

    schema <- compoe_schema(arq)
    conn3 <- conectamorgana(schema)
    expect_identical(conn, conn3)
})