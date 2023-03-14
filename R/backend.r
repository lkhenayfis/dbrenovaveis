########################## FUNCOES BACKEND PARA QUERIES EM BANCO E LOCAIS ##########################

# EXECUCAO DE QUERIES ------------------------------------------------------------------------------

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
#' @param query lista detalhando a query como retornado por \code{\link{parseargs}}
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

    if(class(out)[1] == "try-error") stop(out[1]) else return(out)
}

roda_query.local <- function(conexao, query) {

    query$SELECT <- strsplit(query$SELECT, ",")[[1]]
    query$WHERE  <- lapply(query$WHERE, function(q) {
        q <- gsub(" IN ", " %in% ", q)
        q <- gsub("\\(", "c\\(", q)
        if(grepl("AND", q)) paste0("(", sub(" AND ", ") & (", q), ")") else q
    })
    if(!is.null(query[["ORDER BY"]])) query[["ORDER BY"]] <- strsplit(query[["ORDER BY"]], ",")[[1]]

    oldtz <- Sys.getenv("TZ")
    Sys.setenv("TZ" = "GMT")

    tempart <- checa_particao(conexao, query)

    if(!tempart) {
        out <- try(proc_query_local_spart(conexao, query))
    } else {
        out <- try(proc_query_local_cpart(conexao, query))
    }
    out <- try(corrigeposix(out))

    Sys.setenv("TZ" = oldtz)

    if(class(out)[1] == "try-error") stop(out[1]) else return(out)
}

#' Checa Existencia De Particoes Locais
#' 
#' Avalia se uma tabela local corresponde a conjunto de particoes ou nao
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param query lista detalhando a query como retornado por \code{\link{parseargs}}
#' 
#' @return booleano indicando se a tabela e particionada ou nao

checa_particao <- function(conexao, query) {

    tempart <- attr(conexao, "tempart") && attr(conexao, "particoes")[[query$FROM]]

    return(tempart)
}

#' Leitor De Tabelas Em Mock Bancos
#' 
#' Abstracao para leitura de arquivos csv ou parquet
#' 
#' @param tabela tabela a ser lida, em formato csv ou parquet. Deve sempre ser uma tabela regular
#' 
#' @return data.table contendo a tabela lida

le_tabela_mock <- function(conexao, tabela, ...) {
    rf  <- attr(conexao, "reader_fun")
    arq <- file.path(conexao, paste0(tabela, ".", attr(conexao, "extensao")))
    dat <- rf(arq, ...)
    return(dat)
}

#' Executores Internos De Query Local
#' 
#' Realizam queries em bancos de dados locais, com ou sem particao
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param query lista detalhando a query como retornado por \code{\link{parseargs}}
#' 
#' @return dado recuperado do banco ou erro caso a query nao possa ser realizada
#' 
#' @name query_local

#' @rdname query_local

proc_query_local_spart <- function(conexao, query) {

    dat <- le_tabela_mock(conexao, query$FROM)

    for(q in query$WHERE) {
        vsubset <- eval(str2lang(q), envir = dat)
        dat <- dat[vsubset, ]
    }

    cols <- query$SELECT
    dat <- dat[, ..cols]

    cc_order <- list(quote(setorder), quote(dat))
    for(i in query[["ORDER BY"]]) cc_order <- c(cc_order, list(str2lang(i)))
    eval(as.call(cc_order))

    return(dat)
}

#' @rdname query_local

proc_query_local_cpart <- function(conexao, query) {

    master <- le_tabela_mock(conexao, query$FROM)
    colspart <- colnames(master)
    colspart <- colspart[colspart != "tabela"]

    querymaster <- query[c("SELECT", "FROM", "WHERE")]
    querymaster$SELECT <- "tabela"
    querymaster$WHERE <- querymaster$WHERE[colspart]

    tabelas <- proc_query_local_spart(conexao, querymaster)$tabela

    dat <- lapply(tabelas, function(tabela) {
        querytabela <- query
        querytabela$FROM <- tabela

        proc_query_local_spart(conexao, querytabela)
    })
    dat <- rbindlist(dat)

    return(dat)
}

# INTERPRETACAO DE ARGUMENTOS ----------------------------------------------------------------------

#' Parse Argumentos Das \code{get_funs_quanti}
#' 
#' Interpreta cada argumento, expandindo os vazios para todos os valores possiveis
#' 
#' O comportamento padrao desta funcao e expandir todos os argumentos nao fornecidos (iguais a 
#' \code{NA}) para todas as possibilidades no banco. Por exemplo, se \code{usinas = NA}, este trecho
#' da query retorna todas as usinas disponiveis. 
#' 
#' A exceção desta regra e o argumento \code{horizontes}. Este sera expandido para todos os 
#' horizontes disponiveis para o modelo mais limitado. Isto significa que se ha tres modelos no 
#' banco, indo ate D5, D6 e D7, \code{horizontes} sera expandido para \code{1:5}. A razao disso e
#' que as funcoes \code{get} nao sao feitas para queries de multiplos modelos e horizontes 
#' simultaneamente, dado que este caso de uso e extremamente raro nas aplicacoes de renovaveis da
#' PEM.
#' 
#' O argumento \code{campos} tambem passa por um tratamento especial. Caso nao seja informado, mas
#' \code{modelos} ou \code{horizontes} tenham e indiquem apenas um modelo ou um horizonte, sera
#' removido de \code{campos} as colunas indexadoras dessas informacoes. Caso \code{campos} seja
#' fornecido sem "id_modelo" ou "dia_previsao", mas o usuario tenha informado \code{modelos} ou 
#' \code{horizontes} de modo que mais de um seja requisitado, \code{campos} sera modificado para
#' incluir as colunas indexadoras.
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param tabela um de "verificados" ou "previstos"
#' @param usinas vetor de usinas buscadas. \code{"*"} busca todas; \code{NA} nenhuma
#' @param longitudes vetor de longitudes buscadas. \code{"*"} busca todas; \code{NA} nenhuma
#' @param latitudes vetor de latitudes buscadas. \code{"*"} busca todas; \code{NA} nenhuma
#' @param datahoras string de janela de tempo buscada. \code{"*"} ou \code{NA} retorna todas
#' @param modelos vetor de modelos buscados, so tem uso quando \code{tabela == "previstos"}.
#'     \code{"*"} retorna todos; \code{NA} nenhum
#' @param horizontes vetor de horizontes de previsao buscados, so tem uso quando 
#'     \code{tabela == "previstos"}. \code{"*"} retorna todos; \code{NA} nenhum
#' @param campos vetor de campos da tabela buscados. \code{"*"} ou \code{NA} traz todos
#' 
#' @return lista contendo os trechos de query assoiados a cada argumento da funcao

parseargs <- function(conexao, tabela, usinas = NA, longitudes = NA, latitudes = NA, datahoras = NA,
    modelos = NA, horizontes = NA, campos = NA) {

    extra <- ifelse(tabela == "previstos", "data_hora_previsao", "data_hora")
    if((campos[1] == "*") || is.na(campos[1])) campos <- listacampos(conexao, tabela)

    q_usinas     <- parseargs_usinas(conexao, usinas)
    q_vertices   <- parseargs_vertices(conexao, longitudes, latitudes)
    q_datahoras  <- parseargs_datahoras(datahoras, extra)
    q_modelos    <- parseargs_modelos(conexao, modelos)
    q_horizontes <- parseargs_horizontes(conexao, horizontes)

    ORDERBY <- c("id_usina", "id_vertice", "id_modelo", "dia_previsao", extra)

    campos_full <- listacampos(conexao, tabela)
    mantem <- c(extra, campos)

    if(tabela == "previstos") {
        mantem <- if(attr(q_modelos, "n") == 1) mantem[!(mantem %in% "id_modelo")] else c(mantem, "id_modelo")
        mantem <- if(attr(q_horizontes, "n") == 1) mantem[!(mantem %in% "dia_previsao")] else c(mantem, "dia_previsao")
    }

    mantem <- if(attr(q_usinas, "n") == 1) mantem[!(mantem %in% "id_usina")] else c(mantem, "id_usina")
    mantem <- if(attr(q_vertices, "n") == 1) mantem[!(mantem %in% "id_vertice")] else c(mantem, "id_vertice")
    mantem <- mantem[!grepl("^id$", mantem)]
    campos <- campos_full[campos_full %in% mantem]
    q_campos <- paste0(campos, collapse = ",")

    SELECT <- q_campos
    FROM   <- tabela
    WHERE  <- list(id_usina = q_usinas, id_vertice = q_vertices, id_modelo = q_modelos,
        dia_previsao = q_horizontes, data_hora = q_datahoras)
    WHERE  <- WHERE[!sapply(WHERE, is.na)]
    ORDERBY <- ORDERBY[ORDERBY %in% campos]
    ORDERBY <- paste0(ORDERBY, collapse = ",")

    out <- list(SELECT = SELECT, FROM = FROM, WHERE = WHERE, "ORDER BY" = ORDERBY)

    return(out)
}

# HELPERS ------------------------------------------------------------------------------------------

parseargs_usinas <- function(conexao, usinas) {
    if(is.na(usinas[1]) || usinas[1] == "*") return(structure(NA, "n" = 0))

    usinas <- getusinas(conexao, usinas, campos = "id")[[1]]

    q_usinas <- paste0("id_usina IN (", paste0(usinas, collapse = ", "), ")")
    attr(q_usinas, "n") <- length(usinas)

    return(q_usinas)
}

parseargs_datahoras <- function(datahoras, extra) {
    if((datahoras[1] == "*") || is.na(datahoras[1])) datahoras <- "1000/3999"
    q_datahoras <- parsedatas(datahoras, extra)
    return(q_datahoras)
}

parseargs_modelos <- function(conexao, modelos) {
    if(is.na(modelos[1]) || modelos[1] == "*") return(structure(NA, "n" = 0))

    modelos <- getmodelos(conexao, modelos, campos = "id")[[1]]

    q_modelos <- paste0("id_modelo IN (", paste0(modelos, collapse = ", "), ")")
    attr(q_modelos, "n") <- length(modelos)

    return(q_modelos)
}

parseargs_horizontes <- function(conexao, horizontes) {
    if(is.na(horizontes[1]) || horizontes[1] == "*") return(structure(NA, "n" = 0))

    horizontes <- sub("D|d", "", horizontes)
    horizontes <- as.numeric(ifelse(horizontes == "", "0", horizontes))

    q_horizontes <- paste0("dia_previsao IN (", paste0(horizontes, collapse = ", "), ")")
    attr(q_horizontes, "n") <- length(horizontes)

    return(q_horizontes)
}

parseargs_vertices <- function(conexao, longitudes, latitudes) {

    temlon <- !(is.na(longitudes[1]) || (longitudes[1] == "*"))
    temlat <- !(is.na(latitudes[1])  || (latitudes[1] == "*"))

    if(!temlon && !temlat) return(structure(NA, "n" = 0))

    vertices <- getvertices(conexao, longitudes, latitudes, campos = "id")[[1]]

    q_vertices <- paste0("id_vertice IN (", paste0(vertices, collapse = ", "), ")")
    attr(q_vertices, "n") <- length(vertices)

    return(q_vertices)
}

corrigeposix <- function(dat) {
    coldt <- sapply(dat, function(x) "POSIXct" %in% class(x))
    if(sum(coldt) == 0) return(dat)
    coldt <- names(coldt)[coldt]
    dat[, (coldt) := lapply(.SD, as.numeric), .SDcols = coldt]
    dat[, (coldt) := lapply(.SD, as.POSIXct, origin = "1970-01-01", tz = "GMT"), .SDcols = coldt]
    return(dat)
}

listacampos <- function(conexao, tabela) UseMethod("listacampos")
listacampos.default <- function(conexao, tabela) DBI::dbListFields(conexao, tabela)
listacampos.local   <- function(conexao, tabela) {
    tempart <- checa_particao(conexao, list(FROM = tabela))
    if(!tempart) {
        dat <- le_tabela_mock(conexao, tabela, nrow = 1)
        colnames(dat)
    } else {
        mestre <- le_tabela_mock(conexao, tabela, nrow = 1)
        tabela <- mestre$tabela[1]
        dat    <- le_tabela_mock(conexao, tabela, nrow = 1)
        colnames(dat)
    }
}