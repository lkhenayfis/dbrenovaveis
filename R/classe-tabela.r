#################################### CLASSES DE TABELA E CAMPOS ####################################

#' Construtor Da Classe \code{tabela}
#' 
#' Funcao para geracao de objetos representativos das tabelas em um determinado banco de dados
#' 
#' Esta funcao faz parte do backend do pacote \code{dbrenovaveis}. Atraves dela e possivel 
#' especificar a estrutura de qualquer tabela regular, definindo seus campos e correspondentes 
#' tipos, particoes e outros atributos.
#' 
#' @param nome string indicando o nome da tabela
#' @param campos lista de objetos \code{campo}. Veja \code{\link{new_campos}} para mais detalhes
#' @param uri caminho do diretorio ou bucket contendo o(s) arquivo(s) que compoe(m) a tabela
#' @param tipo_arquivo extensao do(s) arquivo(s) que compoe(m) a tabela
#' @param particoes opcional, vetor de nomes dos campos pelos quais a tabela e particionada
#' @param descricao opcional, breve descricao da tabela e o que contem
#' 
#' @examples 
#' 
#' # contruindo uma tabela nao particionada a partir do dado exemplo do pacote
#' # utilizando um pequeno subset de suas colunas para simplificar o exemplo
#' tab <- new_tabela(
#'     nome = "subbacias",
#'     campos = list(
#'         new_campo("posto", "int"),
#'         new_campo("nome", "string"),
#'         new_campo("codigo", "string")
#'     ),
#'     uri = system.file("extdata/cpart_parquet/subbacias/", package = "dbrenovaveis"),
#'     tipo_arquivo = ".parquet.gzip"
#' )
#' 
#' # construindo uma tabela particionada
#' tab <- new_tabela(
#'     nome = "vazoes",
#'     campos = list(
#'         new_campo("data", "date"),
#'         new_campo("codigo", "string"),
#'         new_campo("vazao", "float")
#'     ),
#'     uri = system.file("extdata/cpart_parquet/vazoes/", package = "dbrenovaveis"),
#'     particoes = c("codigo"),
#'     tipo_arquivo = ".parquet.gzip",
#'     descricao = "Dados de entrada de vazao observada"
#' )
#' 
#' @return objeto descritivo de uma tabela
#' 
#' @seealso Construtor externo \code{\link{schema2tabela}}. Funcao geral para acessar dados das 
#'     tabelas \code{\link{getfromdb}}

new_tabela <- function(nome, campos, uri, tipo_arquivo, particoes = NULL, descricao = NULL) {

    if (missing("uri")) stop("Argumento 'uri' vazio")
    source <- ifelse(grepl("^s3://", uri), "s3", "local")

    if (missing("tipo_arquivo")) stop("Argumento 'tipo_arquivo' vazio")

    if (missing("campos")) stop("Argumento 'campos' vazio")
    if (length(names(campos)) == 0) {
        names(campos) <- sapply(campos, "[[", "nome")
    }

    tabela <- list(nome = nome, campos = campos, particoes = particoes)

    class(tabela) <- c(paste0("tabela_", source), "tabela")
    attr(tabela, "uri") <- uri
    attr(tabela, "tipo_arquivo") <- tipo_arquivo
    attr(tabela, "descricao") <- descricao
    attr(tabela, "reader_func") <- switch_reader_func(tipo_arquivo, source == "s3")
    attr(tabela, "master") <- build_master_unit(tabela)

    return(tabela)
}

#' Construtor Externo
#' 
#' Gera objetos \code{tabela} a partir de um schema.json. Wrapper de \code{new_tabela}
#' 
#' @param schema ou uma lista de schema.json já lido ou o caminho do arquivo
#' 
#' @examples 
#' 
#' # Usando as mesmas tabelas exemplificadas na doc de `new_tabela`
#' tab1 <- schema2tabela(system.file("extdata/cpart_parquet/subbacias/schema.json", package = "dbrenovaveis"))
#' tab2 <- schema2tabela(system.file("extdata/cpart_parquet/vazoes/schema.json", package = "dbrenovaveis"))
#' 
#' @return objeto \code{tabela}

schema2tabela <- function(schema) {

    if (is.character(schema)) {
        rf <- switch_reader_func("json", grepl("^s3", schema))
        schema <- rf(schema)
    }

    campos <- lapply(schema$columns, function(cc) new_campo(cc$name, cc$type))

    new <- new_tabela(schema$name, campos, schema$uri, schema$fileType,
        sapply(schema$partitions, "[[", "name"), schema$description)
}

#' @export

print.tabela <- function(x, ...) {
    campos <- lapply(x$campos, function(x) {
        paste0(x$nome, cli::col_grey(" <", sub("campo_", "", class(x)[1]), ">"))
    })

    cat("Tabela:", x$nome, "\n")
    cat("- Conteudo:", attr(x, "descricao"), "\n")
    cat("- Campos:", paste0(campos, collapse = ", "))
}

# CAMPOS -------------------------------------------------------------------------------------------

#' Construtor Da Classe \code{campo}
#' 
#' Funcao para geracao de objetos representativos de campos em tabelas num determinado banco
#' 
#' Esta classe compoe o coracao da representacao simbolica de tabelas, utilizada dentro de 
#' \code{\link{new_tabela}} para especificacao das colunas. O argumento \code{tipo} permite detalhar
#' o tipo de dados contido neste campo, podendo ser um de:
#' 
#' \itemize{
#' \item{int}
#' \item{float}
#' \item{string}
#' \item{date}
#' \item{datetime}
#' }
#' 
#' Cada \code{tipo} de dado gera um tipo especifico de query e possui funcionalidades proprias 
#' durante o processamento de argumentos numa execucao de funcao para formar a query.
#' 
#' @param nome o nome do campo (nome da coluna na tabela)
#' @param tipo o tipo de dado contido. Veja Detalhes para os tipos suportados e suas implicacoes
#' 
#' @return Objeto da classe \code{campo} contendo especificacao de um campo qualquer
#' 
#' @seealso Construtor de tabelas \code{\link{new_tabela}} para melhor entendimento de seu uso

new_campo <- function(nome, tipo = c("int", "float", "string", "date", "datetime")) {

    tipo <- try(match.arg(tipo), TRUE)
    if (inherits(tipo, "try-error")) {
        msg <- paste0("Tipo do campo '", nome, "' nao permitido -- deve ser um de (",
            paste0(formals(new_campo)$tipo[-1], collapse = ", "), ")")
        stop(msg)
    }

    out <- list(nome = nome)
    class(out) <- c(paste0("campo_", tipo), "campo")

    return(out)
}

# AUXILIARES ---------------------------------------------------------------------------------------

lista_conteudo <- function(tabela) UseMethod("lista_conteudo")

lista_conteudo.tabela_local <- function(tabela) list.files(attr(tabela, "uri"))

lista_conteudo.tabela_s3 <- function(tabela) {
    splitted <- strsplit(attr(tabela, "uri"), "/")[[1]]
    bucket <- do.call(file.path, as.list(c(head(splitted, 3), "")))
    prefix <- do.call(file.path, as.list(c(splitted[-seq(3)], "")))
    out <- aws.s3::get_bucket(bucket, prefix)
    out <- unname(sapply(out, "[[", "Key"))
    out <- sub(prefix, "", out)
    return(out)
}

#' Monta Tabela Mestra
#' 
#' Constroi uma tabela mestra associando particoes as entidades contendo aquele trecho
#' 
#' @param tabela a tabela particionada para qual construir uma mestra
#' 
#' @return tabela mestra

build_master_unit <- function(tabela) {

    arqs <- lista_conteudo(tabela)
    arqs <- sub("\\..*", "", arqs)
    arqs <- arqs[arqs != "schema"]

    ll <- lapply(tabela$particoes, function(x) {
        re <- regexpr(paste0("(?<=", x, "=)(.*?)(?=(-|\\Z))"), arqs, perl = TRUE)
        regmatches(arqs, re)
    })
    names(ll) <- tabela$particoes

    master <- cbind(as.data.table(ll), tabela = arqs)

    return(master)
}

switch_reader_func <- function(extensao, s3 = FALSE) {
    if (grepl("\\.?json", extensao)) {
        if (s3) {
            reader_func <- function(x, ...) aws.s3::s3read_using(FUN = jsonlite::read_json, object = x, ...)
        } else {
            reader_func <- jsonlite::read_json
        }
    } else if (grepl("\\.?csv", extensao)) {
        if (s3) {
            reader_func <- function(x, ...) aws.s3::s3read_using(FUN = data.table::fread, object = x, ...)
        } else {
            reader_func <- data.table::fread
        }
    } else if (grepl("\\.?parquet", extensao)) {
        if (!requireNamespace("arrow", quietly = TRUE)) {
            stop("Pacote 'arrow' e necessario para leitura de arquivos parquet")
        }

        if (s3) {
            reader_func <- function(x, ...) aws.s3::s3read_using(FUN = arrow::read_parquet, object = x, ...)
        } else {
            reader_func <- arrow::read_parquet
        }
    } else {
        stop("arquivos locais de tipo nao suportado")
    }

    return(reader_func)
}