#################################### CLASSES DE TABELA E CAMPOS ####################################

#' Construtor Da Classe \code{tabela}
#' 
#' Funcao para geracao de objetos representativos das tabelas em um determinado banco de dados
#' 
#' Esta funcao faz parte do backend do pacote \code{dbrenovaveis}. Atraves dela e possivel 
#' especeficar a estrutura de qualquer tabela regular, definindo seus campos, tipo de dado por campo
#' e uma conexao com o banco de dados onde ela se encontra.
#' 
#' @param nome string indicando o nome da tabela
#' @param campos lista de objetos \code{campo}. Veja \code{\link{new_campos}} para mais detalhes
#' @param uri caminho completo do diretorio ou no bucket contendo o(s) arquivo(s) que compoe(m)
#'     a tabela
#' @param tipo_arquivo extensao do(s) arquivo(s) que compoe(m) a tabela
#' @param particoes opcional, vetor de nomes dos campos pelos quais a tabela e particionada
#' @param descricao opcional, breve descricao da tabela e o que contem
#' 
#' @examples 
#' 
#' # Para o banco local incluso como exemplo no pacote
#' 
#' dirloc <- system.file("extdata/sempart", package = "dbrenovaveis")
#' conect <- conectalocal(dirloc)
#' 
#' # versao simplificada da tabela de usinas
#' tabusi <- new_tabela(
#'     nome = "usinas",
#'     campos = list(
#'         new_campo("id", "inteiro"),
#'         new_campo("codigo", "string")),
#'     conexao = conect)
#' 
#' # representacao da tabela de verificados
#' tabverif <- new_tabela(
#'     nome = "verificados",
#'     campos = list(
#'         new_campo("id_usina", "inteiro"),
#'         new_campo("data_hora", "data"),
#'         new_campo("vento", "float"),
#'         new_campo("geracao", "float")
#'     ),
#'     conexao = conect)
#' 
#' @return objeto descritivo de uma tabela
#' 
#' @seealso Funcao geral para acessar dados das tabelas \code{\link{getfromdb}}
#' 
#' @export

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
#' Gera objetos \code{tabela} a partir de um schema.json
#' 
#' @param schema ou uma lista de schema.json já lido ou o caminho do arquivo
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
#' Cada tipo de dado gera um tipo especifico de query e possui funcionalidades proprias durante o
#' processamento de argumentos numa execucao de funcao para formar a query.
#' 
#' 
#' @param nome o nome do campo (nome da coluna na tabela)
#' @param tipo o tipo de dado contido. Veja Detalhes para os tipos suportados e suas implicacoes
#' 
#' @examples 
#' 
#' # Retomando os exemplos de 'new_tabela'
#' 
#' dirloc <- system.file("extdata/sempart", package = "dbrenovaveis")
#' conect <- conectalocal(dirloc)
#' 
#' tabusi <- new_tabela(
#'     nome = "usinas",
#'     campos = list(
#'         new_campo("id", "int"),
#'         new_campo("codigo", "string")),
#'     conexao = conect)
#' 
#' # Similar ao exemplo da descricao, vemos que a tabela de verificados nao possui uma coluna 
#' # 'codigo' com a qual pesquisar por usinas, apenas o id inteiro arbitrario.
#' # Ao especificar o campo 'id_usina' com proxy 'codigo' na tabela 'tabusi', o pacote sabera que
#' # uma query sobre usinas na tabela de verificados pode ser feita na coluna 'codigo' de 'tabusi'
#' 
#' tabverif <- new_tabela(
#'     nome = "verificados",
#'     campos = list(
#'         new_campo("id_usina", "int"),
#'         new_campo("data_hora", "date"),
#'         new_campo("vento", "float"),
#'         new_campo("geracao", "float")
#'     ),
#'     conexao = conect)
#' 
#' @return Objeto da classe \code{campo} contendo especificacao de um campo qualquer
#' 
#' @export

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