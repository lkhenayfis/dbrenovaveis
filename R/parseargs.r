# INTERPRETACAO DE ARGUMENTOS ----------------------------------------------------------------------

#' Wrapper De Parses Para Queries
#' 
#' Interpreta argumentos para montar a string da query a ser executada no banco
#' 
#' @param tabela objeto da classe \code{tabela} criado por \code{\link{new_tabela}}
#' @param campos os campos a reter apos a leitura. Por padrao traz todos
#' @param ... subsets a serem aplicados. Veja Detalhes e Exemplos
#' 
#' @return lista contendo os trechos de query para executar no banco
#' 
#' @seealso generica para parse de cada campo individualmente \code{\link{parsearg}}; 

parseargs <- function(tabela, campos = NA, ...) {

    if ((campos[1] == "*") || is.na(campos[1])) campos <- names(tabela$campos)

    subsets <- list(...)
    if (length(subsets) == 0) {
        WHERE <- NULL
    } else {
        WHERE  <- lapply(names(subsets), function(campo) {
            parsearg(tabela$campos[[campo]], subsets[[campo]])
        })
        names(WHERE) <- sapply(names(subsets), function(campo) tabela$campos[[campo]]$nome)
    }

    SELECT <- paste0(campos, collapse = ",")
    FROM   <- tabela$nome

    out <- list(SELECT = SELECT, FROM = FROM, WHERE = WHERE)

    return(out)
}

# PARSES UNITARIOS ---------------------------------------------------------------------------------

#' Generica Para Parse De Argumentos Unicos
#' 
#' Funcao generica dos metodos de parse de argmumentos passados nas queries de tabela
#' 
#' \code{parseargsOLD} e uma funcao generica para processar argumentos passados nas funcoes de query.
#' Cada tipo de campo (data, inteiro, string e etc.) possui um metodo proprio que retorna uma parte
#' do elemento WHERE da query associada ao campo passado.
#' 
#' No caso de campos do tipo \code{campo_data}, \code{valor} deve ser uma string indicando a faixa
#' de datahoras no formato \code{"YYYY[-MM-DD HH:MM:SS]/YYYY[-MM-DD HH:MM:SS]"}. As partes entre
#' colchetes sao opcionais, tanto no limite inicial quanto final.
#' 
#' @param campo objeto da classe \code{campo} gerado por \code{\link{new_campo}}
#' @param valor o valor buscado na tabela para montar a query
#' @param ... demais argumentos que possam existir nos metodos de cada tipo de dado
#' 
#' @examples
#' 
#' tabusi <- new_tabela(
#'     nome = "usinas",
#'     campos = list(
#'         new_campo("id", "inteiro", FALSE),
#'         new_campo("codigo", "string", FALSE)),
#'     conexao = conect)
#' 
#' # parse num campo sem proxy
#' \dontrun{
#' parsearg(tabusi$campos[[1]], codigo = "RNUEM3")
#' }
#' 
#' \dontrun{
#' # parse num campo com proxy
#' campo <- new_campo("id_usina", "inteiro", TRUE, tabusi, "id", "codigo")
#' parsearg(campo, valor = "BAEBAU")
#' }
#' 
#' \dontrun{
#' # parse de datas
#' campo <- new_campo("datahora", "data")
#' parsearg(campo, valor = "2020-01-01/2021-02-13 13:40")
#' }
#' 
#' @return string contendo o trecho de WHERE relacionado ao campo em questao
#' 
#' @export

parsearg <- function(campo, valor, ...) UseMethod("parsearg")

#' @export

parsearg.campo_int <- function(campo, valor, ...) {
    if (is.na(valor[1]) || valor[1] == "*") return(structure(NA, "n" = 0))

    valor_str <- paste0(valor, collapse = ",")
    WHERE     <- paste0(campo$nome, " IN (", valor_str, ")")

    attr(WHERE, "n") <- length(valor)

    return(WHERE)
}

#' @export

parsearg.campo_string <- function(campo, valor, ...) {
    if (is.na(valor[1]) || valor[1] == "*") return(structure(NA, "n" = 0))

    valor_str <- paste0(valor, collapse = "','")
    WHERE <- paste0(campo$nome, " IN ('", valor_str, "')")

    attr(WHERE, "n") <- length(valor)

    return(WHERE)
}

#' @export

# implementacao provisoria
# ainda falta fazer os possiveis subsets por lista ou faixa
parsearg.campo_float <- function(campo, valor, ...) parsearg.campo_int(campo, valor, ...)

#' @export

parsearg.campo_datetime <- function(campo, valor, ...) {

    nome <- campo$nome

    if ((valor[1] == "*") || is.na(valor[1])) valor <- "1000/3999"
    WHERE <- parsedatas(valor, nome)

    return(WHERE)
}

#' @export

parsearg.campo_date <- function(campo, valor, ...) parsearg.campo_datetime(campo, valor)

# HELPERS ------------------------------------------------------------------------------------------

#' Parse De Tipo Data(Hora)
#' 
#' Transforma \code{datahoras} passado as funcoes \code{get*} numa string de query
#' 
#' Esta funcao e responsavel por traduzir as strings de janela temporal \code{datahora} das funcoes
#' \code{\link{get_funs_quant}} em condicionais de WHERE para a query.
#' 
#' Por padrao, todas as queries serao realizadas no padrao \code{data >= lim_1 AND data < lim_2}, 
#' isto e, serao sempre buscados intervalos fechados no inicio e abertos no final. Desta forma todos
#' os padroes de janela podem ser representados da mesma forma, simplificando a funcao.
#' 
#' @param datahoras uma string indicando faixa de tempo, como descrito em \code{\link{get_funs_quant}}
#' @param nome nome do campo para query
#' @param query booleano indicando se deve ser retornada a query ou apenas a expansao das datas
#' 
#' @examples 
#' 
#' \dontrun{
#' cond <- parsedatas("2021", "data_hora")
#' identical(cond, "data_hora >= '2021-01-01 00:00:00' AND data_hora < '2022-01-01 00:00:00'")
#' }
#' 
#' \dontrun{
#' cond <- parsedatas("2020-11-30 12:30", "data_hora")
#' identical(cond, "data_hora >= '2020-11-30 12:30:00' AND data_hora < '2020-11-30 12:30:01'")
#' }
#' 
#' @return se \code{query = TRUE} string contendo a condicao de busca associada a datas na query, do
#'    contrario retorna apenas as datas expandidas

parsedatas <- function(datahoras, nome, query = TRUE) {

    if (!grepl("/", datahoras)) datahoras <- paste0(rep(datahoras, 2), collapse = "/")
    if (grepl("^/", datahoras)) datahoras <- paste0("1000-01-01 00:00:00", datahoras)
    if (grepl("/$", datahoras)) datahoras <- paste0(datahoras, "3999-01-01 00:00:00")

    datahoras <- strsplit(datahoras, "/")[[1]]
    datahoras <- lapply(datahoras, expandedatahora)

    if (!query) return(datahoras)

    datahoras  <- sapply(1:2, function(i) datahoras[[i]][i])
    querydatas <- paste0(nome, " >= '", datahoras[1], "' AND ", nome, " < '", datahoras[2], "'")

    return(querydatas)
}

#' Interpreta As Datahoras Passadas Em \code{datahora}
#' 
#' Transforma as expressoes \code{datahoras} em limites POSIX correspondentes a janela 
#' 
#' Como descrito em \code{\link{get_funs_quant}}, existem diversas maneiras de especificar uma janela de
#' tempo para acesso ao banco de dados. Esta funcao e responsavel por processar as strings de janela
#' temporal passadas la.
#' 
#' Antes de detalhar o processamento, e util apresentar a saida. \code{datahora} pode ser ou um 
#' unico valor ou dois separados pela barra, mas de qualquer forma isto representa uma janela com 
#' comeco e fim. \code{expandedatahora} retorna um vetor contendo as datahoras correspondentes a 
#' estes limites, sendo o limite final exclusivo, isto e, retorna limites para uma busca do tipo
#' \eqn{x \in [lim_1, lim_2)}.
#' 
#' Se \code{datahora} for um valor temporal completo, contendo ate os minutos, entende-se que o 
#' usuario esta buscando exatamente aquele registro e nao uma janela. Neste caso o vetor sera 
#' retornado com \code{datahora} na primeira posicao e \code{datahora} mais um segundo na segunda.
#' Desta forma a busca executada e equivalente a uma busca \eqn{x == datahora}
#' 
#' Quando \code{datahora} e um valor simples incompleto, indicando entao uma janela, a expansao 
#' depende do nivel de detalhe informado. Quando se informa apenas ano, mes ou dia, o vetor 
#' retornado contera este ano, mes, ou dia no primeiro instante de tempo possivel na primeira 
#' posiacao e, na segunda, o mesmo instante somado de um ano, mes ou dia respectivamente. Veja os
#' Exemplos para mais detalhes.
#' 
#' @param datahora uma string indicando faixa de tempo simples, isto e, sem uso da contra barra,
#'     como descrito em \code{\link{get_funs_quant}}
#' 
#' @examples
#' 
#' 
#' \dontrun{
#' faixa <- expandedatahora("2021")
#' identical(faixa, c("2021-01-01 00:00:00", "2022-01-01 00:00:00"))
#' }
#' 
#' \dontrun{
#' faixa <- expandedatahora("2020-05-23 12:30")
#' identical(faixa, c("2020-05-23 12:30:00", "2020-05-23 12:30:01"))
#' }
#' 
#' @return vetor de duas strings, o inicio e final da janela representada por \code{data} hora com
#'     intervalos inclusivo e exclusivo, respectivamente

expandedatahora <- function(datahora) {

    datahora    <- strsplit(datahora, " ")[[1]]
    tem_horario <- length(datahora) == 2

    if (tem_horario) {
        out <- as.POSIXct(paste(datahora[1], datahora[2]), tz = "GMT")
        out <- format(out + 0:1, "%Y-%m-%d %H:%M:%S")
        return(out)
    }

    # caso nao seja de horario completo, comeca as possiveis formas de expansao
    vetor_data <- strsplit(datahora[1], "-")[[1]]
    tem_anomesdia <- length(vetor_data) == 3
    tem_anomes    <- length(vetor_data) == 2
    tem_ano       <- length(vetor_data) == 1

    if (tem_anomesdia) {
        data_aux <- as.Date(datahora[1], format = "%Y-%m-%d")
        out <- paste0(data_aux + 0:1, " 00:00:00")
    } else if (tem_anomes) {
        data_aux <- as.Date(paste0(datahora[1], "-01"), format = "%Y-%m-%d")
        out <- paste0(seq(data_aux, length.out = 2, by = "1 month"), " 00:00:00")
    } else if (tem_ano) {
        data_aux <- as.Date(paste0(datahora[1], "-01-01"), format = "%Y-%m-%d")
        out <- paste0(seq(data_aux, length.out = 2, by = "1 year"), " 00:00:00")
    }

    return(out)
}
