########################## FUNCOES BACKEND PARA QUERIES EM BANCO E LOCAIS ##########################

# EXECUCAO DE QUERIES ------------------------------------------------------------------------------

#' Acesso A Tabelas
#' 
#' Funcao generica para leitura de dados em objetos \code{tabela}
#' 
#' Atraves de \code{...} e possivel especificar subsets para a query. Argumentos extras passados por
#' aqui devem ser nomeados igual ao campo da tabela, ou seu proxy, tomando valor igual ao que deve
#' ser retido na query.
#' 
#' @param conexao objeto de conexao com um banco de dados
#' @param tabela nome da tabela no banco \code{conexao}
#' @param campos os campos a reter apos a leitura. Por padrao traz todos
#' @param ... subsets a serem aplicados. Veja Detalhes e Exemplos
#' 
#' @return \code{data.table} dos dados lidos
#' 
#' @examples 
#' 
#' # usando banco mock de exemplo do pacote
#' arq <- system.file("extdata/cpart_parquet/schema.json", package = "dbrenovaveis")
#' conn <- conectamock(arq)
#' 
#' subbacias <- getfromdb(conn, "subbacias", c("nome", "codigo", "bacia_smap"))
#' 
#' \dontrun{
#' head(subbacias)
#' #>             nome     codigo bacia_smap
#' #> 1: Agua Vermelha  AVERMELHA   GRD_PRNB
#' #> 2:     B. Bonita    BBONITA  Paranazao
#' #> 3:  Baixo Iguacu    BAIXOIG        SUL
#' #> 4:      Capivara   CAPIVARA  Paranazao
#' #> 5:       Colider    COLIDER      Norte
#' #> 6:    Emborcacao EMBORCACAO   GRD_PRNB
#' }
#' 
#' previstos <- getfromdb(conn, "previstos", c("data_previsao", "dia_previsao", "codigo", "qcalc"),
#'     codigo = "AVERMELHA", dia_previsao = 1, data_previsao = "2020")
#' 
#' \dontrun{
#' head(previstos)
#' #>    data_previsao dia_previsao    codigo    qcalc
#' #> 1:    2020-01-01            1 AVERMELHA 327.1693
#' #> 2:    2020-01-02            1 AVERMELHA 299.5275
#' #> 3:    2020-01-03            1 AVERMELHA 283.5529
#' #> 4:    2020-01-04            1 AVERMELHA 283.1786
#' #> 5:    2020-01-05            1 AVERMELHA 289.5164
#' #> 6:    2020-01-06            1 AVERMELHA 326.9825
#' }
#' 
#' @export

getfromdb <- function(conexao, tabela, campos = NA, ...) {

    query <- parseargs(conexao$tabelas[[tabela]], campos, ...)
    out   <- roda_query(conexao, query)

    return(out)
}

#' Executa Queries
#' 
#' Generica para execucao de queries entre os diferentes tipos de conexao
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
#' @param query lista detalhando a query como retornado por \code{\link{parseargs}}
#' 
#' @return dado recuperado do banco ou erro caso a query nao possa ser realizada

roda_query <- function(conexao, query) UseMethod("roda_query")

roda_query.default <- function(conexao, query) {

    query <- collate_query(query)

    oldtz <- Sys.getenv("TZ")
    Sys.setenv("TZ" = "GMT")

    out <- as.data.table(try(dbGetQuery(conexao, query), silent = TRUE))
    out <- corrigeposix(out)

    Sys.setenv("TZ" = oldtz)

    if (class(out)[1] == "try-error") stop(out[1]) else return(out)
}

roda_query.mock <- function(conexao, query) {

    query <- query2subset(query)

    oldtz <- Sys.getenv("TZ")
    Sys.setenv("TZ" = "GMT")

    tempart <- checa_particao(conexao, query)

    if (!tempart) {
        out <- try(proc_query_mock_spart(conexao, query))
    } else {
        out <- try(proc_query_mock_cpart(conexao, query))
    }
    out <- try(corrigeposix(out))

    Sys.setenv("TZ" = oldtz)

    if (class(out)[1] == "try-error") stop(out[1]) else return(out)
}

roda_query.morgana <- function(conexao, query) {

    body <- list(
        database = attr(conexao$tabelas[[query$FROM]], "uri"),
        query = collate_query(query)
    )

    req <- httr2::request("https://x1njnj8nxl.execute-api.us-east-1.amazonaws.com/dev/select/")
    req <- httr2::req_body_json(req, body)
    req <- httr2::req_headers(req, "x-api-key" = attr(conexao, "x_api_key"))

    resp <- httr2::req_perform(req)
    resp <- httr2::resp_body_json(resp)
    resp <- decode_output(resp)

    return(resp)
}

# AUXILIARES ---------------------------------------------------------------------------------------

#' Auxiliar Para Correcao De Datetime
#' 
#' Corrige o fuso horario de colunas de data em um data.table
#' 
#' @param dat data.table com colunas a corrigir corrigir
#' 
#' @return Argumento \code{dat} com colunas POSIXct em fuso horario GMT

corrigeposix <- function(dat) {
    coldt <- sapply(dat, function(x) "POSIXct" %in% class(x))
    if (sum(coldt) == 0) return(dat)
    coldt <- names(coldt)[coldt]
    dat[, (coldt) := lapply(.SD, as.numeric), .SDcols = coldt]
    dat[, (coldt) := lapply(.SD, as.POSIXct, origin = "1970-01-01", tz = "GMT"), .SDcols = coldt]
    return(dat)
}

#' \code{base46} -> \code{data.table}
#' 
#' Converte resposta de uma consulta a api para dado padronizado
#' 
#' @param x vetor de informarcao codificada em base64
#' 
#' @return \code{data.table} convertido

decode_output <- function(x) {
    out <- jsonlite::base64_dec(x$body)
    out <- arrow::read_parquet(out)
    out <- as.data.table(out)
    return(out)
}

#' Compoe Query Completa
#' 
#' Auxiliar para transformar a lista de query por trecho numa unica string
#' 
#' @param query lista de trechos SELECT, FROM e WHERE como retornado por \code{\link{parseargs}}
#' 
#' @return string com a query completa

collate_query <- function(query) {
    query <- query[!sapply(query, is.null)]
    query <- lapply(query, paste0, collapse = " AND ")
    query <- sapply(names(query), function(p) paste(p, query[[p]]))
    query <- paste(query, collapse = " ")
    return(query)
}