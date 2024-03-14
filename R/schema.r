################################### VALIDACAO DE JSONS DE SCHEMA ###################################

#' Le E Interpreta Schema De Banco
#' 
#' Realiza a leitura de um \code{schema.json} para banco, valida e sanitariza seus conteudos se 
#' necessario
#' 
#' Uma das principais sanitarizacoes que esta funcao realiza e o controle de caminhos relativos. O
#' recomendado e que todas as chaves \code{uri} em arquivos schema sejam sempre caminhos absolutos.
#' Por outro lado, para fins de testagem do pacote isto nao funciona pois nao e possivel saber de 
#' antemao aonde o pacote sera instalado.
#' 
#' Quando a funcao e chamada com \code{arq_schema_banco}, e possivel identificar se caminhos 
#' relativos existem e converte-los de acordo caso o argumento seja um caminho absoluto por si; do
#' contrario nao sera possivel resolver os caminhos relativos. Caso seja chamada com 
#' \code{schema_banco}  e esta lista nao contiver um elemento \code{uri}, nao sera possivel 
#' sanitarizar caminhos relativos
#' 
#' @param arq_schema_banco caminho de um arquivo \code{schema.json} para ler
#' @param schema_banco lista contendo um arquivo ja lido. Veja Detalhes
#' 
#' @return lista contendo o schema do banco processado

compoe_schema <- function(arq_schema_banco, schema_banco = NULL) {

    arq_missing <- missing("arq_schema_banco")
    schema_missing <- missing("schema_banco")

    if (arq_missing == schema_missing) stop("Passe apenas un de 'arq_schema_banco' ou 'schema_banco'")

    if (!arq_missing) {
        is_abs_path <- xfun::is_abs_path(arq_schema_banco)

        rf <- switch_reader_func("json", grepl("^s3", arq_schema_banco))
        schema_banco <- rf(arq_schema_banco)
    } else if (!schema_missing) {
        is_abs_path <- FALSE
    }

    tem_uri_root <- !is.null(schema_banco$uri)
    if (!tem_uri_root && is_abs_path) schema_banco$uri <- sub("/schema.json", "", arq_schema_banco)

    schema_banco <- valida_schema_banco(schema_banco)

    schema_banco$tables <- lapply(schema_banco$tables, function(s_t) {
        out <- rf(file.path(s_t$uri, "schema.json"))

        # os schemas de tabelas individuais nao necessariamente vao ter as uris absolutas, ou mesmo
        # certas (embora corrigir isso seja um efeito colateral de corrigir as uris relativas). Por
        # este motivo e feita a substituicao de out$uri pelo caminho de onde out foi lido
        out$uri <- s_t$uri
        valida_schema_tabela(out)
    })

    return(schema_banco)
}

# VALIDADORES --------------------------------------------------------------------------------------

#' Validador De Schemas De Bancos
#' 
#' Valida conteudos de um schema de banco e corrige caminhos relativos de tabelas caso necessario
#' 
#' @param schema uma lista contendo um \code{schema.json} de banco lido
#' 
#' @return lista \code{schema} validada e corrigida, caso necessario, ou erro caso a validacao 
#'     encontre problemas

valida_schema_banco <- function(schema) {

    if (is.null(schema$tables)) {
        stop("Schema do banco nao contem lista de tabelas")
    }

    # mesmo que algo parecido aconteca na funcao acima, e possivel que ainda chegue aqui sem ter uma
    # uri root. Por si so nao e um problema, mas vai ser se as tabelas nao tiverem uris absolutas
    tem_uri_root <- !is.null(schema$uri)
    uri_root_abs <- tem_uri_root && xfun::is_abs_path(schema$uri)
    tab_uris <- sapply(schema$tables, "[[", "uri")

    is_rel_path <- xfun::is_rel_path(tab_uris)
    if (any(is_rel_path)) {
        if (!tem_uri_root) {
            stop("Algumas tabelas possuem 'uri' relativo, porem schema do banco nao possui uma 'uri' root")
        } else {
            tab_uris <- file.path(schema$uri, tab_uris)
            schema$tables <- mapply(tab_uris, schema$tables, FUN = function(u, tt) {
                tt$uri <- u
                tt
            }, SIMPLIFY = FALSE, USE.NAMES = FALSE)
        }
    }

    return(schema)
}

#' Validador De Schemas De Tabelas
#' 
#' Valida conteudos de um \code{schema.json} de tabelas
#' 
#' @param schema uma lista contendo um \code{schema.json} de tabela lido
#' 
#' @return lista \code{schema} validada ou erro, caso a validacao encontre problemas

valida_schema_tabela <- function(schema) {

    # validacao de nome e uri --------------------------------------------

    tem_nome <- !is.null(schema$name)
    if (!tem_nome) stop("Schema nao possui chave 'name'")

    tem_uri <- !is.null(schema$uri)
    if (!tem_uri) stop("Schema nao possui chave 'uri'")
    if (!grepl(paste0(schema$name, "/?$"), schema$uri)) stop("'uri' do schema nao termina em 'name'")

    # validacao de tipo de arquivo ---------------------------------------

    tipo_permitido  <- valida_tipo_arquivo(schema$fileType)
    schema$fileType <- tipo_permitido # substitui para o caso de schema$fileType nao iniciar com '.'

    # validacoes de colunas ----------------------------------------------

    nomes_cols <- lapply(schema$columns, "[[", "name")
    cols_tem_nome <- sapply(nomes_cols, function(nc) !is.null(nc))
    if (!all(cols_tem_nome)) stop("Coluna '", nomes_cols[!cols_tem_nome], "' nao tem chave 'name'")

    cols_tipos_ok <- sapply(schema$columns, function(cc) {
        aux <- try(valida_tipo_campo(cc$type), silent = TRUE)
        !inherits(aux, "try-error")
    })
    if (!all(cols_tipos_ok)) stop("Coluna '", nomes_cols[!cols_tipos_ok], "' possui 'typo' nao permitido")

    return(schema)
}