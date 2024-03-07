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
#' # usina "BAEBAU" em marco de 2020, todos os previstos disponiveis (GFS e ECMWF) horizonte D1
#' getprevisto(conn, "BAEBAU", "2020-03", horizontes = "D1")
#' 
#' # pegando o horizonte D3
#' getprevisto(conn, "BAEBAU", "2020-03", horizontes = "D3")
#' getprevisto(conn, "BAEBAU", "2020-03", horizontes = 3)
#' 
#' # retornando apenas o vento GFS
#' getprevisto(conn, "BAEBAU", "2020-03", modelos = "GFS")
#' 
#' # COMBINADO --------------------------------------------
#' 
#' # pegando vento "RNUEM3" verificado e previstos GFS e ECMWF em fevereiro de 2021, D1
#' getdados(conn, "RNUEM3", "2021-02", "GFS", 1)
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

    query <- parseargsOLD(conexao, "verificados", usinas, NA, NA, datahoras, NA, NA, campos)
    verif <- roda_query(conexao, query)

    return(verif)
}

#' @export 
#' 
#' @rdname get_funs_quant

getprevisto <- function(conexao, usinas = "*", datahoras = "*", modelos = "*", horizontes = "*",
    campos = "vento") {

    query <- parseargsOLD(conexao, "previstos", usinas, NA, NA, datahoras, modelos, horizontes, campos)
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

    query <- parseargsOLD(conexao, tabela, usinas, longitudes, latitudes, datahoras, NA, NA, campos)
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
