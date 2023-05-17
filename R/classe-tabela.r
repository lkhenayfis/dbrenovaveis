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
#' @param conexao objeto de conexao ao banco em que esta a tabela retornado por 
#'     \code{\link{conectabanco}}
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
#'         new_campo("id", "inteiro", FALSE),
#'         new_campo("codigo", "string", FALSE)),
#'     conexao = conect)
#' 
#' # representacao da tabela de verificados
#' tabverif <- new_tabela(
#'     nome = "verificados",
#'     campos = list(
#'         new_campo("id_usina", "inteiro", TRUE, tabusi, "id", "codigo"),
#'         new_campo("data_hora", "data", FALSE),
#'         new_campo("vento", "float", FALSE),
#'         new_campo("geracao", "float", FALSE)
#'     ),
#'     conexao = conect)
#' 
#' @return objeto descritivo de uma tabela
#' 
#' @export

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
