#################################### CLASSES DE TABELA E CAMPOS ####################################

#' Construtor Da Classe \code{tabela}
#' 
#' Funcao para geracao de objetos representativos das tabelas em um determinado banco de dados
#' 
#' Esta funcao faz parte do backend do pacote \code{dbrenovaveis}. Atraves dela e possivel 
#' especificar a estrutura de qualquer tabela regular, definindo seus campos e correspondentes 
#' tipos, particoes e outros atributos.
#' 
#' @param nome string indicando o nome da tabela
#' @param campos lista de objetos \code{campo}. Veja \code{\link{new_campo}} para mais detalhes
#' @param uri caminho do diretorio ou bucket contendo o(s) arquivo(s) que compoe(m) a tabela
#' @param tipo_arquivo extensao do(s) arquivo(s) que compoe(m) a tabela
#' @param particoes opcional, vetor de nomes dos campos pelos quais a tabela e particionada
#' @param descricao opcional, breve descricao da tabela e o que contem
#' 
#' @examples 
#' 
#' # contruindo uma tabela nao particionada a partir do dado exemplo do pacote
#' # utilizando um pequeno subset de suas colunas para simplificar o exemplo
#' tab <- dbrenovaveis:::new_tabela(
#'     nome = "subbacias",
#'     campos = list(
#'         dbrenovaveis:::new_campo("posto", "int"),
#'         dbrenovaveis:::new_campo("nome", "string"),
#'         dbrenovaveis:::new_campo("codigo", "string")
#'     ),
#'     uri = system.file("extdata/cpart_parquet/subbacias/", package = "dbrenovaveis"),
#'     tipo_arquivo = ".parquet.gzip"
#' )
#' 
#' # construindo uma tabela particionada
#' tab <- dbrenovaveis:::new_tabela(
#'     nome = "vazoes",
#'     campos = list(
#'         dbrenovaveis:::new_campo("data", "date"),
#'         dbrenovaveis:::new_campo("codigo", "string"),
#'         dbrenovaveis:::new_campo("vazao", "float")
#'     ),
#'     uri = system.file("extdata/cpart_parquet/vazoes/", package = "dbrenovaveis"),
#'     particoes = c("codigo"),
#'     tipo_arquivo = ".parquet.gzip",
#'     descricao = "Dados de entrada de vazao observada"
#' )
#' 
#' @return objeto descritivo de uma tabela
#' 
#' @seealso Construtor externo \code{\link{schema2tabela}}. Funcao geral para acessar dados das 
#'     tabelas \code{\link{getfromdb}}

new_tabela <- function(nome, campos, uri, tipo_arquivo, particoes = NULL, descricao = NULL) {

    if (missing("uri")) stop("Argumento 'uri' vazio")
    source <- ifelse(grepl("^s3://", uri), "s3", "local")

    if (missing("tipo_arquivo")) stop("Argumento 'tipo_arquivo' vazio")

    if (missing("campos")) stop("Argumento 'campos' vazio")
    if (length(names(campos)) == 0) {
        names(campos) <- sapply(campos, "[[", "nome")
    }

    tabela <- list(nome = nome, campos = campos, particoes = unlist(particoes))

    class(tabela) <- c(paste0("tabela_", source), "tabela")
    attr(tabela, "uri") <- uri
    attr(tabela, "tipo_arquivo") <- tipo_arquivo
    attr(tabela, "descricao") <- descricao
    attr(tabela, "reader_func") <- switch_reader_func(tipo_arquivo, source == "s3")
    attr(tabela, "master") <- build_master_unit(tabela)

    return(tabela)
}

#' Construtor Externo
#' 
#' Gera objetos \code{tabela} a partir de um schema.json. Wrapper de \code{new_tabela}
#' 
#' @param schema ou uma lista de schema.json já lido ou o caminho do arquivo
#' 
#' @examples 
#' 
#' # Usando as mesmas tabelas exemplificadas na doc de `new_tabela`
#' arq1 <- system.file("extdata/cpart_parquet/subbacias/schema.json", package = "dbrenovaveis")
#' tab1 <- dbrenovaveis:::schema2tabela(arq1)
#' arq2 <- system.file("extdata/cpart_parquet/vazoes/schema.json", package = "dbrenovaveis")
#' tab2 <- dbrenovaveis:::schema2tabela(arq2)
#' 
#' @return objeto \code{tabela}

schema2tabela <- function(schema) {

    if (is.character(schema)) {
        rf <- switch_reader_func("json", grepl("^s3", schema))
        schema <- rf(schema)
    }

    campos <- lapply(schema$columns, function(cc) new_campo(cc$name, cc$type))

    new <- new_tabela(schema$name, campos, schema$uri, schema$fileType,
        sapply(schema$partitions, "[[", "name"), schema$description)
}

#' @export

print.tabela <- function(x, ...) {
    campos <- lapply(x$campos, function(x) {
        paste0(x$nome, cli::col_grey(" <", sub("campo_", "", class(x)[1]), ">"))
    })

    cat("Tabela:", x$nome, "\n")
    cat("- Conteudo:", attr(x, "descricao"), "\n")
    cat("- Campos:", paste0(campos, collapse = ", "))
}

# CAMPOS -------------------------------------------------------------------------------------------

#' Construtor Da Classe \code{campo}
#' 
#' Funcao para geracao de objetos representativos de campos em tabelas num determinado banco
#' 
#' Esta classe compoe o coracao da representacao simbolica de tabelas, utilizada dentro de 
#' \code{\link{new_tabela}} para especificacao das colunas. O argumento \code{tipo} permite detalhar
#' o tipo de dados contido neste campo, podendo ser um de:
#' 
#' \itemize{
#' \item{int}
#' \item{float}
#' \item{string}
#' \item{date}
#' \item{datetime}
#' }
#' 
#' Cada \code{tipo} de dado gera um tipo especifico de query e possui funcionalidades proprias 
#' durante o processamento de argumentos numa execucao de funcao para formar a query.
#' 
#' @param nome o nome do campo (nome da coluna na tabela)
#' @param tipo o tipo de dado contido. Veja Detalhes para os tipos suportados e suas implicacoes
#' 
#' @return Objeto da classe \code{campo} contendo especificacao de um campo qualquer
#' 
#' @seealso Construtor de tabelas \code{\link{new_tabela}} para melhor entendimento de seu uso

new_campo <- function(nome, tipo) {

    tipo <- valida_tipo_campo(tipo)
    out <- list(nome = nome)
    class(out) <- c(paste0("campo_", tipo), "campo")

    return(out)
}

valida_tipo_campo <- function(tipo) {
    suport <- c("int", "float", "string", "date", "datetime")
    if (!(tipo %in% suport)) {
        msg <- paste0("Tipo nao permitido -- deve ser um de (", paste0(suport, collapse = ", "), ")")
        stop(msg)
    }
    return(tipo)
}

# AUXILIARES ---------------------------------------------------------------------------------------

#' Listagem De Arquivos Por Tabela
#' 
#' Generica e metodos para listar os arquivos que compoem uma tabela abstrata, local ou no s3
#' 
#' @param tabela objeto de classe \code{tabela} cujos arquivos componentes devem ser listados
#' 
#' @return vetor contendo a lista de caminhos completos, local ou s3, dos arquivos que compoem 
#'     \code{tabela}

lista_conteudo <- function(tabela) UseMethod("lista_conteudo")

#' @rdname lista_conteudo

lista_conteudo.tabela_local <- function(tabela) list.files(attr(tabela, "uri"))

#' @rdname lista_conteudo

lista_conteudo.tabela_s3 <- function(tabela) {
    splitted <- strsplit(attr(tabela, "uri"), "/")[[1]]
    bucket <- do.call(file.path, as.list(c(head(splitted, 3), "")))
    prefix <- do.call(file.path, as.list(c(splitted[-seq(3)], "")))
    out <- aws.s3::get_bucket(bucket, prefix)
    out <- unname(sapply(out, "[[", "Key"))
    out <- sub(prefix, "", out)
    return(out)
}

#' Monta Tabela Mestra
#' 
#' Constroi uma tabela mestra associando particoes as entidades contendo aquele trecho
#' 
#' A tabela mestra nada mais e do que um data.table simples com duas ou mais colunas. Uma destas se
#' chamara \code{tabela} e contera o nome de um arquivo que componha a tabela abstrata. As demais
#' colunas sao nomeadas pelos campos de particionamento e conterao, a cada linha, o elemento daquele
#' campo contido na particao associada
#' 
#' O uso de tabelas mestras nao e estritamente necessario. Pela nomenclatura de particionamento,
#' seria possivel simplesmente procurar nos nomes de arquivos quais sao os que devem ser lidos. Isso
#' funciona tranquilamente quando sao mocks locais, mas no s3 demandaria listagem dos buckets toda
#' vez que uma leitura particionada ocorresse, o que nao e ideal.
#' 
#' @param tabela objeto de classe \code{tabela} para o qual construir uma mestra
#' 
#' @examples 
#' 
#' # para a tabela exemplo 'assimilacao' particionada por 'codigo' e 'dia_assimilacao'
#' arq <- system.file("extdata/cpart_parquet/assimilacao/schema.json", package = "dbrenovaveis")
#' tab <- dbrenovaveis:::schema2tabela(arq)
#' 
#' mestra <- dbrenovaveis:::build_master_unit(tab)
#' 
#' \dontrun{
#' print(mestra)
#' #>       codigo dia_assimilacao                                         tabela
#' #> 1: AVERMELHA               1 assimilacao-codigo=AVERMELHA-dia_assimilacao=1
#' #> 2: AVERMELHA               2 assimilacao-codigo=AVERMELHA-dia_assimilacao=2
#' #> 3:   BAIXOIG               1   assimilacao-codigo=BAIXOIG-dia_assimilacao=1
#' #> 4:   BAIXOIG               2   assimilacao-codigo=BAIXOIG-dia_assimilacao=2
#' }
#' 
#' @return tabela mestra

build_master_unit <- function(tabela) {

    arqs <- lista_conteudo(tabela)
    arqs <- sub("\\..*", "", arqs)
    arqs <- arqs[arqs != "schema"]

    ll <- lapply(tabela$particoes, function(x) {
        re <- regexpr(paste0("(?<=", x, "=)(.*?)(?=(-|\\Z))"), arqs, perl = TRUE)
        regmatches(arqs, re)
    })
    names(ll) <- tabela$particoes

    master <- cbind(as.data.table(ll), tabela = arqs)

    return(master)
}
