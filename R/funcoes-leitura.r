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
    extensao <- valida_tipo_arquivo(extensao)
    inner_reader <- eval(parse(text = paste0("inner", gsub("\\.", "_", extensao))))
    reader_func  <- ifelse(s3, outer_s3(inner_reader), outer_local(inner_reader))
    return(reader_func)
}

valida_tipo_arquivo <- function(tipo) {
    if (!grepl("^\\.", tipo)) tipo <- paste0(".", tipo)
    suport <- c(".csv", ".json", ".parquet", ".parquet.gzip")
    if (!(tipo %in% suport)) {
        msg <- paste0("Tipo de arquivo nao permitido -- deve ser um de (",
            paste0(suport, collapse = ", "), ")")
        stop(msg)
    }

    if (grepl("parquet", tipo) && !requireNamespace("arrow", quietly = TRUE)) {
        stop("Pacote 'arrow' e necessario para leitura de arquivos parquet")
    }

    return(tipo)
}

# FUNCOES DE LEITURA INTERNAS ----------------------------------------------------------------------

inner_json <- function(x, ...) jsonlite::read_json(x, ...)

inner_csv <- function(x, ...) data.table::fread(x, ...)

inner_parquet <- function(x, ...) arrow::read_parquet(x, ...)

inner_parquet_gzip <- function(x, ...) arrow::read_parquet(x, ...)

# FUNCOES DE LEITURA EXTERNAS ----------------------------------------------------------------------

outer_local <- function(inner_fun, ...) inner_fun

outer_s3 <- function(inner_fun, ...) function(x, ...) aws.s3::s3read_using(inner_fun, object = x, ...)