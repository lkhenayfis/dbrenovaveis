########################## FUNCOES BACKEND PARA QUERIES EM BANCO E LOCAIS ##########################

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
#' @param query string da query
#' 
#' @return dado recuperado do banco ou erro caso a query nao possa ser realizada

roda_query <- function(conexao, query) UseMethod("roda_query")

roda_query.default <- function(conexao, query) {

    query <- lapply(query, paste0, collapse = " AND ")
    query <- sapply(names(query), function(p) paste(p, query[[p]]))
    query <- paste(query, collapse = " ")

    oldtz <- Sys.getenv("TZ")
    Sys.setenv("TZ" = "GMT")

    out <- try(dbGetQuery(conexao, query), silent = TRUE)
    coldt <- sapply(out, function(x) "POSIXct" %in% class(x))

    out[, coldt] <- lapply(out[, coldt, drop = FALSE], as.numeric)
    out[, coldt] <- lapply(out[, coldt, drop = FALSE], as.POSIXct, origin = "1970-01-01", tz = "GMT")

    Sys.setenv("TZ" = oldtz)

    if(class(out) == "try-error") stop(out[1]) else return(out)
}

roda_query.local <- function(conexao, query) {
    NA
}

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
#' @param usinas vetor de usinas buscadas. \code{NA} busca todas
#' @param datahoras string de janela de tempo buscada. \code{NA} retorna todas
#' @param modelos vetor de modelos buscados, so tem uso quando \code{tabela == "previstos"}.
#'     \code{NA} retorna todos
#' @param horizontes vetor de horizontes de previsao buscados, so tem uso quando 
#'     \code{tabela == "previstos"}. \code{NA} retorna todos
#' @param campos vetor de campos da tabela buscados
#' 
#' @return lista contendo os trechos de query assoiados a cada argumento da funcao

parseargs <- function(conexao, tabela, usinas = NA, datahoras = NA, modelos = NA, horizontes = NA,
    campos = NA) {

    extra <- ifelse(tabela == "verificados", "data_hora", "data_hora_previsao")

    # Parse escolha de usinas --------------------------------------------

    if(is.na(usinas[1])) {
        q_usinas <- NULL
    } else {
        if(usinas[1] == "*") {
            usinas <- getusinas(conexao, campos = "id")[, 1]
        } else {
            usinas <- toupper(usinas)
            usinas <- getusinas(conexao, usinas, campos = "id")[, 1]
        }
        q_usinas <- paste0("id_usina IN (", paste0(usinas, collapse = ", "), ")")
    }

    # Parse escolha de datas ---------------------------------------------

    if((datahoras[1] == "*") || is.na(datahoras[1])) datahoras <- "0001/9999"
    q_datahoras <- parsedatas(datahoras, extra)

    # Parse escolha de modelos -------------------------------------------

    if(is.na(modelos[1])) {
        q_modelos <- NULL
    } else {
        if(modelos[1] == "*") {
            modelos <- getmodelos(conexao, campos = "id")[, 1]
        } else {
            modelos <- toupper(modelos)
            modelos <- getmodelos(conexao, modelos, campos = "id")[, 1]
        }
        q_modelos <- paste0("id_modelo IN (", paste0(modelos, collapse = ", "), ")")
    }

    # Parse escolha de horizontes ----------------------------------------

    if(is.na(horizontes[1])) {
        q_horizontes <- NULL
    } else {
        if(horizontes[1] == "*") {
                horizontes <- getmodelos(conexao, campos = "horizonte_previsao")[, 1]
                horizontes <- seq_len(min(horizontes))
        } else {
            horizontes <- sub("D|d", "", horizontes)
            horizontes <- as.numeric(ifelse(horizontes == "", "0", horizontes))
        }
        q_horizontes <- paste0("dia_previsao IN (", paste0(horizontes, collapse = ", "), ")")
    }

    # Montagem final -----------------------------------------------------

    orderby <- c("id_usina", "id_modelo", "dia_previsao", extra)

    if((campos[1] == "*") || is.na(campos[1])) {
        campos <- DBI::dbListFields(conexao, tabela)
    } else {
        campos <- c(extra, campos)
    }

    if(tabela == "previstos") {
        campos  <- if(length(modelos) == 1)  campos[!grepl("id_modelo", campos)] else c(campos, "id_modelo")
        campos  <- if(length(horizontes) == 1) campos[!grepl("dia_previsao", campos)] else c(campos, "dia_previsao")
    }

    campos <- if(length(usinas) == 1) campos[!grepl("id_usina", campos)] else c(campos, "id_usina")
    campos <- campos[!grepl("^id$", campos)]
    campos <- campos[!duplicated(campos)]
    q_campos <- paste0(campos, collapse = ",")

    SELECT <- q_campos
    FROM   <- tabela
    WHERE  <- list(usinas = q_usinas, datahoras = q_datahoras,
        modelos = q_modelos, horizontes = q_horizontes)
    WHERE <- WHERE[!sapply(WHERE, is.null)]
    ORDERBY <- orderby[!sapply(list(q_usinas, q_modelos, q_horizontes, q_datahoras), is.null)]
    ORDERBY <- paste0(ORDERBY, collapse = ",")

    out <- list(SELECT = SELECT, FROM = FROM, WHERE = WHERE, "ORDER BY" = ORDERBY)

    return(out)
}
