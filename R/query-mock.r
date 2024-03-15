################################# FUNCOES PARA QUERY DE MOCK BANCOS ################################

#' Transforma Query Em Expressoes De Subset
#' 
#' Auxiliar para conversao de strings de query em db para expressoes de subset em data.table
#' 
#' @param query lista contendo termos SELECT, WHERE e ORDERBY de uma query
#' 
#' @return mesma lista com os termos em questao convertidos para expressoes de subset

query2subset <- function(query) {
    query$SELECT <- strsplit(query$SELECT, ",")[[1]]
    query$WHERE  <- lapply(query$WHERE, function(q) {
        q <- gsub(" IN ", " %in% ", q)
        q <- gsub("\\(", "c\\(", q)
        if (grepl("AND", q)) paste0("(", sub(" AND ", ") & (", q), ")") else q
    })
    if (!is.null(query[["ORDER BY"]])) query[["ORDER BY"]] <- strsplit(query[["ORDER BY"]], ",")[[1]]
    return(query)
}

#' Checa Existencia De Particoes Locais
#' 
#' Avalia se uma tabela local corresponde a conjunto de particoes ou nao
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param query lista detalhando a query como retornado por \code{\link{parseargs}}
#' 
#' @return booleano indicando se a tabela e particionada ou nao

checa_particao <- function(conexao, query) {
    tabfrom <- conexao$tabelas[[query$FROM]]
    tempart <- !is.null(tabfrom$particoes)
    return(tempart)
}

#' Leitor De Tabelas Em Mock Bancos
#' 
#' Abstracao para leitura de arquivos csv ou parquet
#' 
#' @param tabela objeto de classe \code{tabela} cujos arquivos componentes devem ser listados
#' @param arquivo nome do arquivo componente de \code{tabela} a ser lido, sem extensao
#' @param ... demais argumentos que possam ser passados para o leitor interno
#' 
#' @return data.table contendo a tabela lida

le_tabela_mock <- function(tabela, arquivo, ...) {
    rf  <- attr(tabela, "reader_fun")
    arq <- file.path(attr(tabela, "uri"), paste0(arquivo, attr(tabela, "tipo_arquivo")))
    dat <- rf(arq, ...)
    return(dat)
}

#' Executores Internos De Query Local
#' 
#' Realizam queries em bancos de dados locais, com ou sem particao
#' 
#' @param conexao objeto de conexao ao banco retornado por \code{\link{conectabanco}}
#' @param query lista detalhando a query como retornado por \code{\link{parseargs}}
#' 
#' @return dado recuperado do banco ou erro caso a query nao possa ser realizada
#' 
#' @name query_local

#' @rdname query_local

proc_query_mock_spart <- function(conexao, query) {

    # quando vem de proc_query_mock_cpart, FROM e um vetor de duas poscoes, indicando a tabela
    # abstrata de onde ler na primeira e o arquivo componente na segunda
    # a implementacao e feita com head e tail porque, no caso de tabelas nao particionadas, FROM tem
    # somente uma posicao mesmo (com [1] e [2] causa erro)
    dat <- le_tabela_mock(
        conexao$tabelas[[head(query$FROM, 1)]],
        tail(query$FROM, 1)
    )

    for (q in query$WHERE) {
        vsubset <- eval(str2lang(q), envir = dat)
        dat <- dat[vsubset, ]
    }

    cols <- query$SELECT
    dat <- dat[, .SD, .SDcols = cols]

    cc_order <- list(quote(setorder), quote(dat))
    for (i in query[["ORDER BY"]]) cc_order <- c(cc_order, list(str2lang(i)))
    eval(as.call(cc_order))

    return(dat)
}

#' @rdname query_local

proc_query_mock_cpart <- function(conexao, query) {

    master <- attr(conexao$tabelas[[query$FROM]], "master")
    colspart <- colnames(master)
    colspart <- colspart[colspart != "tabela"]

    querymaster <- query[c("SELECT", "FROM", "WHERE")]
    querymaster$SELECT <- "tabela"
    if (length(querymaster$WHERE) > 0) querymaster$WHERE <- querymaster$WHERE[colspart]
    querymaster$WHERE <- querymaster$WHERE[sapply(querymaster$WHERE, length) > 0]

    for (q in querymaster$WHERE) {
        vsubset <- eval(str2lang(q), envir = master)
        master <- master[vsubset, ]
    }

    tabelas <- master$tabela

    dat <- lapply(tabelas, function(tabela) {
        querytabela <- query
        querytabela$FROM <- c(query$FROM, tabela)

        proc_query_mock_spart(conexao, querytabela)
    })
    dat <- rbindlist(dat)

    return(dat)
}
