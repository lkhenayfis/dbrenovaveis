################################### VALIDACAO DE JSONS DE SCHEMA ###################################

valida_schema_banco <- function(schema) {

    if (is.null(schema$tables)) {
        stop("Schema do banco nao contem lista de tabelas")
    }

    tem_uri_root <- !is.null(schema$uri)
    uri_root_abs <- tem_uri_root && xfun::is_abs_path(schema_banco$uri)
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

valida_schema_tabela <- function(schema) {

    # validacao de nome e uri --------------------------------------------

    tem_nome <- !is.null(schema$name)
    if (!tem_nome) stop("Schema nao possui chave 'name'")

    tem_uri <- !is.null(schema$uri)
    if (!tem_uri) stop("Schema nao possui chave 'uri'")
    if (!grepl(paste0(schema$name, "/?$"), schema$uri)) stop("'uri' do schema nao termina em 'name'")

    # validacao de tipo de arquivo ---------------------------------------

    tipo_permitido <- valida_tipo_arquivo(schema$fileType)
    schema <- tipo_permitido # substitui para o caso de schema$fileType nao iniciar com '.'

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