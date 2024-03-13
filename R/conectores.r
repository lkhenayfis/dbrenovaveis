################################# FUNCOES GERAIS USADAS NO PACOTE ##################################

#' Conexao Com Bancos
#' 
#' Gera a conexao com o banco \code{banco} utilizando as credenciais de \code{usuario}
#' 
#' ESTA FUNCAO NAO VEM SENDO SUPORTADA HA ALGUMAS VERSOES
#' 
#' Esta funcao e um simples wrapper para facilitacao da conexao com bancos de dados, utilizando
#' as credenciais previamente registradas e salvas pelas funcoes apropriadas. 
#' 
#' @param usuario a tag associada a credenciais previamente regristradas com
#'     \code{\link{registra_credenciais}}
#' @param banco a tag associada a informacoes de banco previamente regristradas com
#'     \code{\link{registra_banco}}
#' 
#' @examples 
#' 
#' # supondo usuario 'user' e banco 'db' ja registrados
#' 
#' \dontrun{
#' # conecta ao banco com credenciais de 'user'
#' conn <- conectabanco("user", "db")
#' 
#' # listando todas as tabelas no banco
#' dbListTables(conn)
#' 
#' # listando campos (colunas) de uma determinada tabela
#' dbListFields(conn, "nome_da_tabela")
#' }
#' 
#' @return objeto de conexao ao banco. Veja \code{\link[DBI]{dbConnect}} para mais detalhes
#' 
#' @import DBI RPostgreSQL
#' 
#' @export

conectabanco <- function(usuario, banco) {
    usuario <- readRDS(file.path(Sys.getenv("dbrenovaveis-cachedir"), paste0("user_", usuario, ".rds")))
    banco   <- readRDS(file.path(Sys.getenv("dbrenovaveis-cachedir"), paste0("db_", banco, ".rds")))

    conn <- dbConnect(
        drv = dbDriver("PostgreSQL"),
        user = usuario[[1]], password = usuario[[2]],
        host = banco[[1]], port = banco[[2]], dbname = banco[[3]]
    )

    return(conn)
}

#' Conexao Com Mock Bancos
#' 
#' Gera a conexao com um mock banco, correspondente a um diretorio local ou s3
#' 
#' Os bancos mock de \code{dbrenovaveis} correspondem a arquivos de dados, em `csv` ou `parquet`, em
#' diretorios locais ou buckets no s3 dentro. Para correto funcionamento desta implementacao se 
#' espera uma certa estrutura de arquivos e diretorios de tal modo que o pacote consiga encontrar
#' os dados relevantes. Recomenda-se ler a vignette
#' `vignette("estrutura-mock", package = "dbrenovaveis")` para maiores detalhes a respeito desta
#' estrutura de mock banco.
#' 
#' A conexao com bancos mock possui apenas um argumento de entrada: `schema`. Este e ou o caminho a
#' um arquivo json explicitando a estrutura geral do banco ou uma lista contendo o este json ja 
#' lido (a estrutura de schema.json se encontra detalhada na vignette supracitada).
#' 
#' @param schema lista contendo o schema do banco, correspondente aos conteudos de um arquivo
#'     \code{schema.json} para banco, ou o caminho de um arquivo deste tipo
#' 
#' @examples 
#' 
#' # conexao com o mock banco exemplo do pacote
#' 
#' # passando o diretorio
#' arq_schema <- system.file("extdata/cpart_parquet/schema.json", package = "dbrenovaveis")
#' conn1 <- conectamock(arq_schema)
#' 
#' # passando o a lista de schema ja lido
#' schema <- jsonlite::read_json(arq_schema)
#' conn2 <- conectamock(schema)
#' 
#' \dontrun{
#' identical(conn1, conn2)
#' }
#' 
#' @return objeto de conexao com o mock banco
#' 
#' @export

conectamock <- function(schema) {

    if (is.character(schema)) {
        rf <- switch_reader_func("json", grepl("^s3", schema))
        schema <- rf(schema)
    }

    tabelas <- lapply(schema$tables, function(tab) {
        tab_schema <- file.path(tab$uri, "schema.json")
        rf <- switch_reader_func("json", grepl("^s3", tab_schema))
        schema2tabela(rf(tab_schema))
    })
    names(tabelas) <- sapply(tabelas, "[[", "nome")

    out <- list(tabelas = tabelas)
    class(out) <- "mock"

    return(out)
}
