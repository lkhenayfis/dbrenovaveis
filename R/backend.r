########################## FUNCOES BACKEND PARA QUERIES EM BANCO E LOCAIS ##########################

# EXECUCAO DE QUERIES ------------------------------------------------------------------------------

#' Acesso A Tabelas
#' 
#' Funcao generica para leitura de dados em objetos \code{tabela}
#' 
#' Atraves de \code{...} e possivel especificar subsets para a query. Argumentos extras passados por
#' aqui devem ser nomeados igual ao campo da tabela, ou seu proxy, tomando valor igual ao que deve
#' ser retido na query.
#' 
#' @param tabela objeto da classe \code{tabela} criado por \code{\link{new_tabela}}
#' @param campos os campos a reter apos a leitura. Por padrao traz todos
#' @param ... subsets a serem aplicados. Veja Detalhes e Exemplos
#' 
#' @return \code{data.table} dos dados lidos
#' 
#' @examples 
#' 
#' # lendo dados verificados
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
#' \dontrun{
#' # note que nao se especifica "id_usina", mas sim o proxy "codigo"
#' getfromdb(tabverif, campos = "geracao", data_hora = "/2021-01-01 10:00:00", codigo = "BAEBAU")
#' }
#' 
#' @export

getfromdb <- function(conexao, tabela, campos = NA, ...) {

    query <- parseargs(tabela, campos, ...)
    out   <- roda_query(conexao, query)

    return(out)
}

#' Executa Queries
#' 
#' Funcao para execucao de queries nas \code{\link{get_funs_quant}}
#' 
#' Existe um problema na conversao de datas nas viradas de horario de verao, dando erro na query.
#' Teoricamente isso pode ser resolvido passando tz = "GMT" para as.POSIXlt, porem o DBI nao da
#' esse nivel de controle.
#' 
#' Para contornar o problema se torna necessario fazer uma maracutaia de trocar o tz do R no momento
#' da query e retorna-lo ao valor original depois. Ao retornar o valor, o R desloca as datas, entao
#' e preciso modificar o atributo tzone da coluna de datas de volta para GMT
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param query lista detalhando a query como retornado por \code{\link{parseargsOLD}}
#' 
#' @return dado recuperado do banco ou erro caso a query nao possa ser realizada

roda_query <- function(conexao, query) UseMethod("roda_query")

roda_query.default <- function(conexao, query) {

    query <- lapply(query, paste0, collapse = " AND ")
    query <- sapply(names(query), function(p) paste(p, query[[p]]))
    query <- paste(query, collapse = " ")

    oldtz <- Sys.getenv("TZ")
    Sys.setenv("TZ" = "GMT")

    out <- as.data.table(try(dbGetQuery(conexao, query), silent = TRUE))
    out <- corrigeposix(out)

    Sys.setenv("TZ" = oldtz)

    if (class(out)[1] == "try-error") stop(out[1]) else return(out)
}

roda_query.mock <- function(conexao, query) {

    query <- query2subset(query)

    oldtz <- Sys.getenv("TZ")
    Sys.setenv("TZ" = "GMT")

    tempart <- checa_particao(conexao, query)

    if (!tempart) {
        out <- try(proc_query_mock_spart(conexao, query))
    } else {
        out <- try(proc_query_mock_cpart(conexao, query))
    }
    out <- try(corrigeposix(out))

    Sys.setenv("TZ" = oldtz)

    if (class(out)[1] == "try-error") stop(out[1]) else return(out)
}

# AUXILIARES ---------------------------------------------------------------------------------------

#' Auxiliar Para Correcao De Datetime
#' 
#' Corrige o fuso horario de colunas de data em um data.table
#' 
#' @param dat data.table com colunas a corrigir corrigir
#' 
#' @return Argumento \code{dat} com colunas POSIXct em fuso horario GMT

corrigeposix <- function(dat) {
    coldt <- sapply(dat, function(x) "POSIXct" %in% class(x))
    if (sum(coldt) == 0) return(dat)
    coldt <- names(coldt)[coldt]
    dat[, (coldt) := lapply(.SD, as.numeric), .SDcols = coldt]
    dat[, (coldt) := lapply(.SD, as.POSIXct, origin = "1970-01-01", tz = "GMT"), .SDcols = coldt]
    return(dat)
}