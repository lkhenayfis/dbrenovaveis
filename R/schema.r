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
