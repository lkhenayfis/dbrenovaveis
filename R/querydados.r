################################## FUNCOES PARA ACESSO AOS DADOS ###################################

#' Leitura De Dados No Banco
#' 
#' Le informacoes das tabelas "verificados" e "previstos" selecionando por valores especificados
#' 
#' \code{getverificado} le dados da tabela "verificados", \code{getprevisto} da tabela "previstos"
#' e \code{getdados} busca as informacoes nas duas e retorna a informacao combinada em um mesmo
#' dado.
#' 
#' Os argumentos \code{campos} nao devem incluir a coluna de data/hora, pois esta sempre sera 
#' retornada por todas as funcoes. A selecao de todas as colunas na tabela pode ser feita atraves de
#' \code{campos = "*"}. Caso este argumento seja fornecido \code{""}, \code{getverificado} e
#' \code{getprevisto} abortam com erro. O mesmo acontece para \code{getdados} se ambos os argumentos
#' \code{campos_*} forem fornecidos vazios, porem nao se apenas um deles o for.
#' 
#' \bold{Argumento \code{datahoras}}:
#' 
#' O argumento \code{datahoras} permite a especificacao de datahoras de forma bastante flexivel. Ha
#' duas formas principais de especificacao: valor unico ou uma janela.
#' 
#' A especificacao em valor unico e feita passando uma string de data ou datahora no formato padrao
#' (\code{"YYYY-mm-dd"} para datas e \code{"YYYY-mm-dd HH:MM"} para datahoras), possivelmente 
#' pariciais, isto e, sem conter todos os niveis. Desta forma, pode ser pedido um mes de determinado
#' ano (\code{"YYYY-mm"}), ou dia do mes de um ano (\code{"YYYY-mm-dd"}).
#' 
#' Serao buscados na tabela todos os registros cujo valor no campo de tempo corresponda ao argumento 
#' ATE O NIVEL DE DETALHE ESPECIFICADO. Isto significa que, caso \code{datahoras = "2021-01-01"}, 
#' serao retornados todos os registros semi-horarios cujo dia e 2021-01-01. O mesmo vale para 
#' especificacoes parciais, como apenas ano-mes. A execucao com \code{datahora = "2020-04"} retorna
#' todos os registros em abril de 2020.
#' 
#' A segunda forma de uso deste argumento corresponde a especificacao de janelas. Isto pode ser 
#' feito informando duas datahoras, possivelmente parciais tais quais descrito no paragrafo 
#' anterior, separadas de uma barra tal qual \code{"YYYY-mm-dd HH:MM/YYYY-mm-dd HH:MM"}. A expansao 
#' de cada parte da janela sera feita exatamente da mesma forma como descrito para valores 
#' singulares, e serao retornados todos os registros no intervalo especificado INCLUINDO os limites.
#' Assim, se \code{datahoras = "2020-05/2021-02-03 03:00"}, serao retornados todos os registros de 
#' maio de 2020 ate as tres da manha do dia 3 de fevereiro de 2021. Veja os Exemplos.
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param usinas opcional, vetor de strings com codigo das usinas cujas informacoes serao buscadas
#' @param longitudes opcional, vetor de strings com longitudes cujas informacoes serao buscadas
#' @param latitudes opcional, vetor de strings com latitudes cujas informacoes serao buscadas
#' @param datahoras string indicando faixa de tempo para ler da tabela
#' @param modelos opcional, vetor strings com nome dos modelos cujas informacoes serao buscadas
#' @param horizontes opcional, vetor de inteiros ou strings do tipo "DX" indicando o horizonte de 
#'     previsao
#' @param campos vetor de strings indicando quais campos (colunas) devem ser lidos. Ver Detalhes
#' @param campos_verif vetor de strings indicando quais campos (colunas) devem ser lidos da tabela
#'     "verificados". Ver Detalhes
#' @param campos_prev vetor de strings indicando quais campos (colunas) devem ser lidos da tabela
#'     "previstos". Ver Detalhes
#' @param modo tipo de reanalise a ser buscada; \code{"grade"} corresponde ao dado bruto em grade e
#'     \code{"interpolado"} aos dados ja interpolados e indexados por \code{id_usina}
#' 
#' @examples 
#' 
#' # supondo usuario 'user' e banco 'db' ja registrados
#' 
#' \dontrun{
#' # conecta ao banco com credenciais de 'user'
#' conn <- conectabanco("user", "db")
#' 
#' # VERIFICADOS ------------------------------------------
#' 
#' # leitura da usina "BAEBAU" em janeiro de 2021
#' getverificado(conn, "BAEBAU", "2021-01")
#' 
#' # leitura da usina "BAEBAU" das 10 da manha do dia primeiro de janeiro de 2021 as 16 do dia 2
#' getverificado(conn, "BAEBAU", "2021-01-01 10:00/2021-01-02 16:00")
#' 
#' # leitura da usina "RNUEM3" de 2020/04/23 a 2021/07/04, incluindo geracao verificada
#' getverificado(conn, "RNUEM3", "2020-04-23/2021-07-04", c("vento", "geracao"))
#' 
#' # PREVISTOS --------------------------------------------
#' 
#' # usina "BAEBAU" em marco de 2020, todos os previstos disponiveis (gfs e ecmwf) horizonte D1
#' getprevisto(conn, "BAEBAU", "2020-03", horizontes = "D1")
#' 
#' # pegando o horizonte D3
#' getprevisto(conn, "BAEBAU", "2020-03", horizontes = "D3")
#' getprevisto(conn, "BAEBAU", "2020-03", horizontes = 3)
#' 
#' # retornando apenas o vento gfs
#' getprevisto(conn, "BAEBAU", "2020-03", modelos = "GFS")
#' 
#' # COMBINADO --------------------------------------------
#' 
#' # pegando vento "RNUEM3" verificado e previstos gfs e ecmwf em fevereiro de 2021, D1
#' getdados(conn, "RNUEM3", "2021-02", "gfs", 1)
#' 
#' }
#' 
#' @seealso \code{\link{get_funs_quali}} para funcoes de leitura das tabelas qualitativas
#' 
#' @return \code{data.frame} contendo as informacoes buscadas
#' 
#' @name get_funs_quant
NULL

#' @export 
#' 
#' @rdname get_funs_quant

getverificado <- function(conexao, usinas = "*", datahoras = "*", campos = "vento") {

    query <- parseargs(conexao, "verificados", usinas, NA, NA, datahoras, NA, NA, campos)
    verif <- roda_query(conexao, query)

    return(verif)
}

#' @export 
#' 
#' @rdname get_funs_quant

getprevisto <- function(conexao, usinas = "*", datahoras = "*", modelos = "*", horizontes = "*",
    campos = "vento") {

    query <- parseargs(conexao, "previstos", usinas, NA, NA, datahoras, modelos, horizontes, campos)
    prev  <- roda_query(conexao, query)

    return(prev)
}

#' @export 
#' 
#' @rdname get_funs_quant

getreanalise <- function(conexao, usinas = "*", longitudes = "*", latitudes = "*", datahoras = "*",
    campos = "vento", modo = c("grade", "interpolado")) {

    modo <- match.arg(modo)
    tabela <- paste0("reanalise_", modo)

    query <- parseargs(conexao, tabela, usinas, longitudes, latitudes, datahoras, NA, NA, campos)
    prev  <- roda_query(conexao, query)

    return(prev)
}

#' @export 
#' 
#' @rdname get_funs_quant

getdados <- function(conexao, usinas, datahoras, modelos, horizontes, campos_verif = c("vento"),
    campos_prev = c("vento")) {

    tem_campos_verif <- (length(campos_verif) > 1) || (campos_verif != "")
    tem_campos_prev  <- (length(campos_prev) > 1) || (campos_prev != "")

    if(!tem_campos_prev & !tem_campos_verif) {
        stop("Ambos os argumentos 'campos_*' estao vazios")
    } else if(tem_campos_prev & !tem_campos_verif) {
        out <- getprevisto(conexao, usinas, datahoras, modelos, horizontes, campos_prev)
    } else if(!tem_campos_prev & tem_campos_verif) {
        out <- getverificado(conexao, usinas, datahoras, campos_verif)
    } else {
        verif <- getverificado(conexao, usinas, datahoras, campos_verif)
        prev  <- getprevisto(conexao, usinas, datahoras, modelos, horizontes, campos_prev)
        out   <- merge(verif, prev, by = 1, suffixes = c("_verif", "_prev"))
    }

    return(out)
}

# HELPERS ------------------------------------------------------------------------------------------

#' Parse Argumento \code{datahoras}
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

    if(!grepl("/", datahoras)) datahoras <- paste0(rep(datahoras, 2), collapse = "/")
    if(grepl("^/", datahoras)) datahoras <- paste0("0001-01-01 00:00:00", datahoras)
    if(grepl("/$", datahoras)) datahoras <- paste0(datahoras, "9999-01-01 00:00:00")

    datahoras <- strsplit(datahoras, "/")[[1]]
    datahoras <- lapply(datahoras, expandedatahora)

    if(!query) return(datahoras)

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

    if(tem_horario) {
        out <- as.POSIXct(paste(datahora[1], datahora[2]), tz = "GMT")
        out <- format(out + 0:1, "%Y-%m-%d %H:%M:%S")
        return(out)
    }

    # caso nao seja de horario completo, comeca as possiveis formas de expansao
    vetor_data <- strsplit(datahora[1], "-")[[1]]
    tem_anomesdia <- length(vetor_data) == 3
    tem_anomes    <- length(vetor_data) == 2
    tem_ano       <- length(vetor_data) == 1

    if(tem_anomesdia) {
        data_aux <- as.Date(datahora[1], format = "%Y-%m-%d")
        out <- paste0(data_aux + 0:1, " 00:00:00")
    } else if(tem_anomes) {
        data_aux <- as.Date(paste0(datahora[1], "-01"), format = "%Y-%m-%d")
        out <- paste0(seq(data_aux, length.out = 2, by = "1 month"), " 00:00:00")
    } else if(tem_ano) {
        data_aux <- as.Date(paste0(datahora[1], "-01-01"), format = "%Y-%m-%d")
        out <- paste0(seq(data_aux, length.out = 2, by = "1 year"), " 00:00:00")
    }

    return(out)
}