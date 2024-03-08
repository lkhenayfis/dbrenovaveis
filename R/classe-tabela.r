#################################### CLASSES DE TABELA E CAMPOS ####################################

#' Construtor Da Classe \code{tabela}
#' 
#' Funcao para geracao de objetos representativos das tabelas em um determinado banco de dados
#' 
#' Esta funcao faz parte do backend do pacote \code{dbrenovaveis}. Atraves dela e possivel 
#' especeficar a estrutura de qualquer tabela regular, definindo seus campos, tipo de dado por campo
#' e uma conexao com o banco de dados onde ela se encontra.
#' 
#' @param nome string indicando o nome da tabela
#' @param campos lista de objetos \code{campo}. Veja \code{\link{new_campos}} para mais detalhes
#' @param uri caminho completo do diretorio ou no bucket contendo o(s) arquivo(s) que compoe(m)
#'     a tabela
#' @param tipo_arquivo extensao do(s) arquivo(s) que compoe(m) a tabela
#' @param particoes opcional, vetor de nomes dos campos pelos quais a tabela e particionada
#' @param descricao opcional, breve descricao da tabela e o que contem
#' 
#' @examples 
#' 
#' # Para o banco local incluso como exemplo no pacote
#' 
#' dirloc <- system.file("extdata/sempart", package = "dbrenovaveis")
#' conect <- conectalocal(dirloc)
#' 
#' # versao simplificada da tabela de usinas
#' tabusi <- new_tabela(
#'     nome = "usinas",
#'     campos = list(
#'         new_campo("id", "inteiro"),
#'         new_campo("codigo", "string")),
#'     conexao = conect)
#' 
#' # representacao da tabela de verificados
#' tabverif <- new_tabela(
#'     nome = "verificados",
#'     campos = list(
#'         new_campo("id_usina", "inteiro"),
#'         new_campo("data_hora", "data"),
#'         new_campo("vento", "float"),
#'         new_campo("geracao", "float")
#'     ),
#'     conexao = conect)
#' 
#' @return objeto descritivo de uma tabela
#' 
#' @seealso Funcao geral para acessar dados das tabelas \code{\link{getfromdb}}
#' 
#' @export

new_tabela <- function(nome, campos, uri, tipo_arquivo, particoes = NULL, descricao = NULL) {

    if (missing("uri")) stop("Argumento 'uri' vazio")
    if (missing("tipo_arquivo")) stop("Argumento 'tipo_arquivo' vazio")

    if (missing("campos")) stop("Argumento 'campos' vazio")
    if (length(names(campos)) == 0) {
        names(campos) <- sapply(campos, "[[", "nome")
    }

    tabela <- list(nome = nome, campos = campos)

    class(tabela) <- "tabela"
    attr(tabela, "uri") <- uri
    attr(tabela, "tipo_arquivo") <- tipo_arquivo
    attr(tabela, "particoes") <- particoes
    attr(tabela, "descricao") <- descricao

    return(tabela)
}

#' Construtor Externo
#' 
#' Gera objetos \code{tabela} a partir de um schema.json
#' 
#' @param schema ou uma lista de schema.json já lido ou o caminho do arquivo
#' 
#' @return objeto \code{tabela}

schema2tabela <- function(schema) {

    if (is.character(schema)) schema <- jsonlite::read_json(schema)

    campos <- lapply(schema$columns, function(cc) new_campo(cc$name, cc$type))

    new <- new_tabela(schema$name, campos, schema$uri, schema$fileType,
        unlist(schema$partitions), schema$description)
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
#' Cada tipo de dado gera um tipo especifico de query e possui funcionalidades proprias durante o
#' processamento de argumentos numa execucao de funcao para formar a query.
#' 
#' 
#' @param nome o nome do campo (nome da coluna na tabela)
#' @param tipo o tipo de dado contido. Veja Detalhes para os tipos suportados e suas implicacoes
#' 
#' @examples 
#' 
#' # Retomando os exemplos de 'new_tabela'
#' 
#' dirloc <- system.file("extdata/sempart", package = "dbrenovaveis")
#' conect <- conectalocal(dirloc)
#' 
#' tabusi <- new_tabela(
#'     nome = "usinas",
#'     campos = list(
#'         new_campo("id", "int"),
#'         new_campo("codigo", "string")),
#'     conexao = conect)
#' 
#' # Similar ao exemplo da descricao, vemos que a tabela de verificados nao possui uma coluna 
#' # 'codigo' com a qual pesquisar por usinas, apenas o id inteiro arbitrario.
#' # Ao especificar o campo 'id_usina' com proxy 'codigo' na tabela 'tabusi', o pacote sabera que
#' # uma query sobre usinas na tabela de verificados pode ser feita na coluna 'codigo' de 'tabusi'
#' 
#' tabverif <- new_tabela(
#'     nome = "verificados",
#'     campos = list(
#'         new_campo("id_usina", "int"),
#'         new_campo("data_hora", "date"),
#'         new_campo("vento", "float"),
#'         new_campo("geracao", "float")
#'     ),
#'     conexao = conect)
#' 
#' @return Objeto da classe \code{campo} contendo especificacao de um campo qualquer
#' 
#' @export

new_campo <- function(nome, tipo = c("int", "float", "string", "date", "datetime")) {

    tipo <- try(match.arg(tipo), TRUE)
    if (inherits(tipo, "try-error")) {
        msg <- paste0("Tipo do campo '", nome, "' nao permitido -- deve ser um de (",
            paste0(formals(new_campo)$tipo[-1], collapse = ", "), ")")
        stop(msg)
    }

    out <- list(nome = nome)
    class(out) <- c("campo", paste0("campo_", tipo))

    return(out)
}
