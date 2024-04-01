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
#' lido (a estrutura de schema.json se encontra detalhada na vignette supracitada). O schema de 
#' pode conter uma chave opcional \code{uri}. Ela e opcional pois so seria utilizada no caso das
#' \code{uri} das tabelas serem caminhos relativos. Caso \code{schema} seja passado como um caminho
#' e o json lido nao possua essa chave, sera adicionada como igual ao caminho \code{schema}.
#' 
#' @param schema lista contendo o schema do banco, correspondente aos conteudos de um arquivo
#'     \code{schema.json} para banco, ou o caminho de um arquivo deste tipo ou diretorio que o 
#'     contenha. Veja Detalhes
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
#' schema$uri <- sub("/schema.json", "", arq_schema)
#' conn2 <- conectamock(schema)
#' 
#' \dontrun{
#' identical(conn1, conn2)
#' }
#' 
#' @return objeto de conexao com o mock banco
#' 
#' @export

conectamock <- function(schema) new_mock(schema)

#' Conexao Com S3 Via Morgana
#' 
#' Gera uma conexao mock a um banco no s3, porem realizando queries atraves da engine morgana
#' 
#' A conexao com um banco S3 via morgana e essencialmente a mesma coisa que um mock banco no s3, com
#' um unico elemento de diferenca sendo a necessidade de uma chave de API para uso das funcoes.
#' 
#' O argumento \code{x_api_key} existe para receber esta chave. Por padrao sera buscada uma variavel
#' de ambiente \code{"X_API_KEY"} na secao para este argumento. Esta e a abordagem recomendada, de
#' modo que informacoes pessoais e sensiveis nao ficam expostas hardcoded.
#' 
#' O objeto de saida e, para todos os efeitos, identico a uma conexao mock com o s3. Possui apenas 
#' um atributo adicional que e a chave de api passada originalmente
#' 
#' @param schema lista contendo o schema do banco, correspondente aos conteudos de um arquivo
#'     \code{schema.json} para banco, ou o caminho de um arquivo deste tipo ou diretorio que o 
#'     contenha. Veja \code{\link{conectamock}}
#' @param x_api_key chave de api para uso das funcoes que compoem o morgana na aws. Veja Detalhes
#' 
#' @return objeto de conexao com o mock banco via morgana

conectamorgana <- function(schema, x_api_key = Sys.getenv("X_API_KEY")) {

    if (!requireNamespace("httr2", quietly = TRUE)) {
        stop("Conexao como cliente do morgana exige pacote 'httr2'")
    }

    if (x_api_key == "") stop("Nao foi possivel encontrar uma chave de API -- veja '?conectamorgana'")

    out <- new_mock(schema, TRUE)
    class(out) <- c("morgana", class(out))
    attr(out, "x_api_key") <- x_api_key

    return(out)
}

#' Construtor Interno De Mocks
#' 
#' Construtor utilizado para geracao de objetos conexao com mocks e morgana
#' 
#' @param schema lista contendo o schema do banco, correspondente aos conteudos de um arquivo
#'     \code{schema.json} para banco, ou o caminho de um arquivo deste tipo ou diretorio que o 
#'     contenha.
#' @param morgana booleano indicando se a conexacao e via morgana ou nao
#' 
#' @return objeto da classe \code{mock}

new_mock <- function(schema, morgana = FALSE) {

    is_char <- is.character(schema)
    is_file <- is_char && grepl("schema\\.json$", schema)

    if (is_char && !is_file) schema <- file.path(schema, "schema.json")
    if (is_char) schema <- compoe_schema(schema) else schema <- compoe_schema(, schema)

    tabelas <- lapply(schema$tables, schema2tabela, no_master = morgana)
    names(tabelas) <- sapply(tabelas, "[[", "nome")

    out <- list(tabelas = tabelas)
    class(out) <- "mock"
    attr(out, "uri") <- schema$uri

    return(out)
}

#' @export 

print.mock <- function(x, ...) {
    cat("* Banco 'mock' com tabelas: \n\n")
    for (t in x$tabelas) {
        print(t)
        cat("\n")
    }
}