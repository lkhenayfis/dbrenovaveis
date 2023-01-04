################################ ACESSO AS INFORMACOES QUALITATIVAS ################################

#' Leitura De Tabelas Qualitativas
#' 
#' Busca informacoes nas tabelas descritivas das informacoes no banco
#' 
#' \code{getusinas} retorna a tabela "usinas" do banco, um dataframe contendo as colunas 
#' especificadas em \code{campos}
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param usinas opcional, vetor de strings com codigo das usinas cujas informacoes serao buscadas
#' @param longitudes opcional, vetor de strings com longitudes cujas informacoes serao buscadas
#' @param latitudes opcional, vetor de strings com latitudes cujas informacoes serao buscadas
#' @param modelos opcional, vetor strings com nome dos modelos cujas informacoes serao buscadas
#' @param tipo o tipo de modelo a ser pego: "previsao" para os modelos meteorologicos e "correcao"
#'     para os modelos de correcao de vento
#' @param campos vetor de strings indicando quais campos (colunas) devem ser lidos
#' 
#' @return tabelas qualitativas lidas
#' 
#' @seealso \code{\link{get_funs_quant}} para funcoes de leitura das tabelas quantitativas
#' 
#' @name get_funs_quali
NULL

#' @export 
#' 
#' @rdname get_funs_quali

getusinas <- function(conexao, usinas, campos = "*") {

    if(campos[1] == "*") campos <- listacampos(conexao, "usinas")
    campos <- do.call(paste0, list(campos, collapse = ","))

    SELECT <- campos
    FROM   <- "usinas"

    if(missing(usinas)) {
        WHERE <- NULL
    } else {
        usinas <- do.call(paste0, list(usinas, collapse = "', '"))
        usinas <- paste0("('", usinas, "')")
        WHERE  <- paste0("codigo IN ", usinas)
    }

    query <- list(SELECT = SELECT, FROM = FROM, WHERE = WHERE)
    query <- query[!sapply(query, is.null)]
    out <- roda_query(conexao, query)

    return(out)
}

#' @export 
#' 
#' @rdname get_funs_quali

getmodelos <- function(conexao, modelos, tipo = "previsao", campos = "*") {

    if(campos[1] == "*") campos <- listacampos(conexao, paste0("modelos_", tipo))
    campos <- do.call(paste0, list(campos, collapse = ","))

    SELECT <- campos
    FROM   <- paste0("modelos_", tipo)

    if(missing(modelos)) {
        WHERE <- NULL
    } else {
        modelos <- do.call(paste0, list(modelos, collapse = "', '"))
        modelos <- paste0("('", modelos, "')")
        WHERE   <- paste0("nome IN ", modelos)
    }

    query <- list(SELECT = SELECT, FROM = FROM, WHERE = WHERE)
    query <- query[!sapply(query, is.null)]
    out <- roda_query(conexao, query)

    return(out)
}

#' @export 
#' 
#' @rdname get_funs_quali

getvertices <- function(conexao, longitudes = "*", latitudes = "*", campos = "*") {

    if(campos[1] == "*") campos <- listacampos(conexao, "vertices")
    campos <- do.call(paste0, list(campos, collapse = ","))

    SELECT <- campos
    FROM   <- "vertices"

    temlong <- longitudes[1] != "*"
    temlat  <- latitudes[1] != "*"

    if(temlong & !temlat) {
        longitudes <- paste0(longitudes, collapse = ",")
        WHERE <- list(paste0("longitude IN (", longitudes, ")"))
    } else if(!temlong & temlat) {
        latitudes <- paste0(latitudes, collapse = ",")
        WHERE <- list(paste0("latitude IN (", latitudes, ")"))
    } else if(temlong & temlat) {
        WHERE <- mapply(longitudes, latitudes, FUN = function(lon, lat) {
            paste0("longitude IN (", lon, ") AND latitude IN (", lat, ")")
        })
    } else {
        WHERE <- list(NULL)
    }

    out <- lapply(WHERE, function(w) {
        query <- list(SELECT = SELECT, FROM = FROM, WHERE = w)
        query <- query[!sapply(query, is.null)]
        roda_query(conexao, query)
    })
    out <- rbindlist(out)

    return(out)
}