############################### FUNCOES PARA CACHING DE INFORMACOES ################################

#' Funcoes Para Caching De Informacoes
#' 
#' Registro de informacoes tanto dos bancos quanto de credenciais de usuario
#' 
#' @param usuario string contendo o nome de usuario utilizado no acesso ao banco
#' @param senha string contendo a senha utilizada no acesso ao banco
#' @param host string contendo o endereco de ip no qual o banco esta hospedado
#' @param port string contendo a porta para acesso
#' @param database string contendo o nome do banco a ser acessado
#' @param tag um identificador do conjunto de informacoes sendo registradas
#' 
#' @return salva as informacoes como arquivo binario em um diretorio do pacote
#' 
#' @name cache_funs
NULL

#' @rdname cache_funs

registra_credenciais <- function(usuario, senha, tag) {
    cachedir <- Sys.getenv("dbrenovaveis-cachedir")

    out <- list(usuario, senha)
    saveRDS(out, file.path(cachedir, paste0("user_", tag, ".rds")))
}

#' @rdname cache_funs

registra_banco <- function(host, port, database, tag) {
    cachedir <- Sys.getenv("dbrenovaveis-cachedir")

    out <- list(host, port, database)
    saveRDS(out, file.path(cachedir, paste0("db_", tag, ".rds")))
}

#' Listagem De Registros Salvos
#' 
#' Mostra tags dos registros salvos na maquina
#' 
#' @return vetor de tags de usuarios ou bancos registrados
#' 
#' @name list_cache_funs

#' @rdname list_cache_funs

lista_credenciais <- function() {
    cachedir <- Sys.getenv("dbrenovaveis-cachedir")
    list.files(cachedir, pattern = "^user_")
}

#' @rdname list_cache_funs

lista_bancos <- function() {
    cachedir <- Sys.getenv("dbrenovaveis-cachedir")
    list.files(cachedir, pattern = "^db_")
}