#################################### CLASSES DE TABELA E CAMPOS ####################################

#' Construtor Da Classe \code{tabela}
#' 
#' Construtor interno da classe, nao deve ser usado diretamente pelo usuario
#' 
#' @param nome string indicando o nome da tabela
#' @param campos lista de objetos \code{campos}
#' @param conexao objeto de conexao ao banco em que esta a tabela retornado por 
#'     \code{\link{conectabanco}}
#' 
#' @return objeto descritivo de uma tabela

new_tabela <- function(nome, campos, conexao) {

    tabela <- list(nome = nome, campos = campos)

    attr(tabela, "conexao") <- conexao
    class(tabela) <- "tabela"

    return(tabela)
}

# CAMPOS -------------------------------------------------------------------------------------------

new_campo <- function(nome, tipo, foreignkey = list(has = FALSE, ref = NULL, campo = NULL, proxy = NULL)) {

    campo <- list(nome = nome)

    hasforeignkey <- foreignkey$has
    if(hasforeignkey) {
        if(is.null(foreignkey$ref)) stop("foreignkey nao possui elemento 'ref' indicando tabela referencia")
        if(is.null(foreignkey$campo)) stop("foreignkey nao possui elemento 'campo' indicando campo na 'ref'")
        if(is.null(foreignkey$proxy)) stop("foreignkey nao possui elemento 'ref' indicando proxy na 'ref'")
        campo <- c(campo, foreignkey)
    }

    attr(campo, "hasforeignkey") <- hasforeignkey
    class(campo) <- c("campo", paste0("campo_", tipo))

    return(campo)
}
