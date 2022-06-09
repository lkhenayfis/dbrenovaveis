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

    campos <- do.call(paste0, list(toupper(campos), collapse = ","))

    SELECT <- paste0("SELECT ", campos)
    FROM   <- "FROM usinas"

    if(missing(usinas)) {
        WHERE <- ""
    } else {
        usinas <- do.call(paste0, list(toupper(usinas), collapse = "', '"))
        usinas <- paste0("('", usinas, "')")
        WHERE  <- paste0("WHERE codigo IN ", usinas)
    }

    query <- paste(SELECT, FROM, WHERE)
    out <- dbGetQuery(conexao, query)

    return(out)
}

#' @export 
#' 
#' @rdname get_funs_quali

getmodelos <- function(conexao, modelos, tipo = "previsao", campos = "*") {

    campos <- do.call(paste0, list(toupper(campos), collapse = ","))

    SELECT <- paste0("SELECT ", campos)
    FROM   <- paste0("FROM modelos_", tipo)

    if(missing(modelos)) {
        WHERE <- ""
    } else {
        modelos <- do.call(paste0, list(toupper(modelos), collapse = "', '"))
        modelos <- paste0("('", modelos, "')")
        WHERE   <- paste0("WHERE nome IN ", modelos)
    }

    query <- paste(SELECT, FROM, WHERE)
    out <- dbGetQuery(conexao, query)

    return(out)
}