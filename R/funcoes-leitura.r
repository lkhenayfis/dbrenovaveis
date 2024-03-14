################################### LEITURA DE ARQUIVOS GENERICA ###################################

#' Seleciona Funcao Leitora Por Tipo E Fonte De Arquivo
#' 
#' Para uma dada \code{extensao} de arquivo e sua origem (local ou s3) retorna uma funcao para 
#' leitura do mesmo
#' 
#' @param extensao string indicando a extensao de arquivo, preferencialmente precedida de "."
#' @param s3 booleano indidicano se o arquivo esta no s3 ou nao
#' 
#' @examples 
#' 
#' # para leitura de um json
#' fun1 <- switch_reader_func("json", FALSE)
#' identical(fun1, jsonlite::read_json)
#' 
#' # para leitura de um csv
#' fun2 <- switch_reader_func(".csv", FALSE)
#' identical(fun2, data.table::fread)
#' 
#' @return funcao cujo primeiro argumento e o caminho (local ou s3) do arquivo a ser lido e retorna
#'     a leitura executada

switch_reader_func <- function(extensao, s3 = FALSE) {
    if (grepl("\\.?json", extensao)) {
        if (s3) {
            reader_func <- function(x, ...) aws.s3::s3read_using(FUN = jsonlite::read_json, object = x, ...)
        } else {
            reader_func <- jsonlite::read_json
        }
    } else if (grepl("\\.?csv", extensao)) {
        if (s3) {
            reader_func <- function(x, ...) aws.s3::s3read_using(FUN = data.table::fread, object = x, ...)
        } else {
            reader_func <- data.table::fread
        }
    } else if (grepl("\\.?parquet", extensao)) {
        if (!requireNamespace("arrow", quietly = TRUE)) {
            stop("Pacote 'arrow' e necessario para leitura de arquivos parquet")
        }

        if (s3) {
            reader_func <- function(x, ...) aws.s3::s3read_using(FUN = arrow::read_parquet, object = x, ...)
        } else {
            reader_func <- arrow::read_parquet
        }
    } else {
        stop("arquivos locais de tipo nao suportado")
    }

    return(reader_func)
}
