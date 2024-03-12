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

    query$SELECT <- strsplit(query$SELECT, ",")[[1]]
    query$WHERE  <- lapply(query$WHERE, function(q) {
        q <- gsub(" IN ", " %in% ", q)
        q <- gsub("\\(", "c\\(", q)
        if (grepl("AND", q)) paste0("(", sub(" AND ", ") & (", q), ")") else q
    })
    if (!is.null(query[["ORDER BY"]])) query[["ORDER BY"]] <- strsplit(query[["ORDER BY"]], ",")[[1]]

    oldtz <- Sys.getenv("TZ")
    Sys.setenv("TZ" = "GMT")

    tempart <- checa_particao(conexao, query)

    if (!tempart) {
        out <- try(proc_query_local_spart(conexao, query))
    } else {
        out <- try(proc_query_local_cpart(conexao, query))
    }
    out <- try(corrigeposix(out))

    Sys.setenv("TZ" = oldtz)

    if (class(out)[1] == "try-error") stop(out[1]) else return(out)
}

#' Checa Existencia De Particoes Locais
#' 
#' Avalia se uma tabela local corresponde a conjunto de particoes ou nao
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param query lista detalhando a query como retornado por \code{\link{parseargsOLD}}
#' 
#' @return booleano indicando se a tabela e particionada ou nao

checa_particao <- function(conexao, query) {
    tabfrom <- conexao$tabelas[[query$FROM]]
    tempart <- !is.null(tabfrom$particoes)
    return(tempart)
}

#' Leitor De Tabelas Em Mock Bancos
#' 
#' Abstracao para leitura de arquivos csv ou parquet
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param tabela tabela a ser lida, em formato csv ou parquet. Deve sempre ser uma tabela regular
#' @param ... demais argumentos que possam ser passados para o leitor interno
#' 
#' @return data.table contendo a tabela lida

le_tabela_mock <- function(conexao, tabela, ...) {
    tabela <- file.path(sub("-.*", "", tabela), paste0(tabela, attr(conexao, "extensao")))
    rf  <- attr(conexao, "reader_fun")
    arq <- file.path(conexao$uri, paste0(tabela))
    dat <- rf(arq, ...)
    return(dat)
}

#' Executores Internos De Query Local
#' 
#' Realizam queries em bancos de dados locais, com ou sem particao
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param query lista detalhando a query como retornado por \code{\link{parseargsOLD}}
#' 
#' @return dado recuperado do banco ou erro caso a query nao possa ser realizada
#' 
#' @name query_local

#' @rdname query_local

proc_query_local_spart <- function(conexao, query) {

    dat <- le_tabela_mock(conexao, query$FROM)

    for (q in query$WHERE) {
        vsubset <- eval(str2lang(q), envir = dat)
        dat <- dat[vsubset, ]
    }

    cols <- query$SELECT
    dat <- dat[, ..cols]

    cc_order <- list(quote(setorder), quote(dat))
    for (i in query[["ORDER BY"]]) cc_order <- c(cc_order, list(str2lang(i)))
    eval(as.call(cc_order))

    return(dat)
}

#' @rdname query_local

proc_query_local_cpart <- function(conexao, query) {

    master <- attr(conexao$tabelas[[query$FROM]], "master")
    colspart <- colnames(master)
    colspart <- colspart[colspart != "tabela"]

    querymaster <- query[c("SELECT", "FROM", "WHERE")]
    querymaster$SELECT <- "tabela"
    if (length(querymaster$WHERE) > 0) querymaster$WHERE <- querymaster$WHERE[colspart]
    querymaster$WHERE <- querymaster$WHERE[sapply(querymaster$WHERE, length) > 0]

    for (q in querymaster$WHERE) {
        vsubset <- eval(str2lang(q), envir = master)
        master <- master[vsubset, ]
    }

    tabelas <- master$tabela

    dat <- lapply(tabelas, function(tabela) {
        querytabela <- query
        querytabela$FROM <- tabela

        proc_query_local_spart(conexao, querytabela)
    })
    dat <- rbindlist(dat)

    return(dat)
}

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