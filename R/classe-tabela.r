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

    if(length(names(campos)) == 0) {
        nomescampos <- sapply(campos, function(cc) {
            if(attr(cc, "foreignkey")[[1]]) attr(cc, "foreignkey")[["proxy"]] else cc$nome
        })

        names(campos) <- nomescampos
    }

    tabela <- list(nome = nome, campos = campos)

    attr(tabela, "conexao") <- conexao
    class(tabela) <- "tabela"

    return(tabela)
}

# CAMPOS -------------------------------------------------------------------------------------------

new_campo <- function(nome, tipo, foreignkey = FALSE, ref = NULL, campo = NULL, proxy = NULL) {

    tipo <- switch(tipo,
        "inteiro" = "discreto",
        "string" = "discreto",
        "float" = "continuo",
        "data" = "data")

    if(is.null(tipo)) stop("'tipo' nao esta dentro dos valores permitidos -- Veja '?new_campo'")

    if(foreignkey) {
        if(is.null(ref)) stop("nao foi passado argumento 'ref' indicando tabela referencia")
        if(is.null(campo)) stop("nao foi passado argumento 'campo' indicando campo na 'ref'")
        if(is.null(proxy)) stop("nao foi passado argumento 'ref' indicando proxy na 'ref'")
    }

    out <- list(nome = nome)
    attr(out, "foreignkey") <- list(has = foreignkey, ref = ref, campo = campo, proxy = proxy)
    class(out) <- c("campo", paste0("campo_", tipo))

    return(out)
}
