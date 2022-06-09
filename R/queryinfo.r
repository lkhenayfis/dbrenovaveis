################################ ACESSO AS INFORMACOES QUALITATIVAS ################################

#' Leitura De Tabelas Qualitativas
#' 
#' Busca informacoes nas tabelas descritivas das informacoes no banco
#' 
#' \code{getusinas} retorna a tabela "usinas" do banco, um dataframe contendo as colunas 
#' especificadas em \code{campos}
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
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

getusinas <- function(conexao, campos) {

    if(missing(campos)) campos <- "*"
    query <- paste0("SELECT ", campos, " FROM usinas")
    out <- dbGetQuery(conexao, query)

    return(out)
}

#' @export 
#' 
#' @rdname get_funs_quali

getmodelosprev <- function(conexao, campos) {

    if(missing(campos)) campos <- "*"
    query <- paste0("SELECT ", campos, " FROM modelos_previsao")
    out <- dbGetQuery(conexao, query)

    return(out)
}