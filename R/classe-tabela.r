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
#' @param conexao objeto de conexao ao banco em que esta a tabela retornado por 
#'     \code{\link{conectabanco}}
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
#'         new_campo("id", "inteiro", FALSE),
#'         new_campo("codigo", "string", FALSE)),
#'     conexao = conect)
#' 
#' # representacao da tabela de verificados
#' tabverif <- new_tabela(
#'     nome = "verificados",
#'     campos = list(
#'         new_campo("id_usina", "inteiro", TRUE, tabusi, "id", "codigo"),
#'         new_campo("data_hora", "data", FALSE),
#'         new_campo("vento", "float", FALSE),
#'         new_campo("geracao", "float", FALSE)
#'     ),
#'     conexao = conect)
#' 
#' @return objeto descritivo de uma tabela
#' 
#' @export

new_tabela <- function(nome, campos, conexao) {

    if(length(names(campos)) == 0) {
        nomescampos <- sapply(campos, function(cc) {
            if(attr(cc, "foreignkey")[[1]]) attr(cc, "foreignkey")[["proxy"]] else cc$nome
        })

        names(campos) <- nomescampos
    }

    tabela <- list(nome = nome, campos = campos)

    attr(tabela, "conexao") <- conexao
    class(tabela) <- "tabela"

    return(tabela)
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
#' \item{inteiro}
#' \item{string}
#' \item{float}
#' \item{data}
#' }
#' 
#' Cada tipo de dado gera um tipo especifico de query e possui funcionalidades proprias durante o
#' processamento de argumentos numa execucao de funcao para formar a query.
#' 
#' Todos os argumentos seguintes estao associados ao comportamento quando o campo especificado e uma
#' chave estrangeira, isto e, uma coluna de indices atrelada a outra tabela. \code{foreignkey}
#' permite informar se o campo e ou nao deste tipo. Caso seja \code{FALSE}, todos os arumentos 
#' restantes sao ignorados e nao precisam ser especificados.
#' 
#' Do contrario, \code{ref} deve corresponder a um objeto da classe \code{tabela}, gerado por
#' \code{\link{new_tabela}} indicando em que tabela se encontra o dado de referencia. 
#' \code{camporef} deve ser uma string indicando o nome do campo, na tabela de referencia, em que
#' se encontra a informacao original.
#' 
#' Por fim, \code{proxy} e um argumento especial utilizado para flexibilizacao de alguns
#' comportamentos. E possivel que o usuario queria pesquisar em uma tabela, por exemplo de dados
#' verificados, por usinas por seu codigo, apesar de estarem indexadas por indices inteiros simples.
#' Atraves do arguemento \code{proxy}, pode ser especificada uma coluna na tabela \code{ref} na qual
#' sera feita a pesquisa estrangeira, de modo que ainda se retornem os valores da coluna 
#' \code{camporef}, necessarios para o subset apropriado. Veja os Exemplos para mais detalhes
#' 
#' @param nome o nome do campo (nome da coluna na tabela)
#' @param tipo o tipo de dado contido. Veja Detalhes para os tipos suportados e suas implicacoes
#' @param foreignkey booleano indicando se este campo e uma chave estrangeira
#' @param ref objeto \code{tabela} na qual se encontra a chave estrangeira. Ver Detalhes
#' @param camporef nome do campo da chave estrangeira na tabela de referencia. Ver Detalhes
#' @param proxy nome de um campo proxy para a busca de chave estrangeira. Ver Detalhes
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
#'         new_campo("id", "inteiro", FALSE),
#'         new_campo("codigo", "string", FALSE)),
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
#'         new_campo("id_usina", "inteiro", TRUE, tabusi, "id", "codigo"),
#'         new_campo("data_hora", "data", FALSE),
#'         new_campo("vento", "float", FALSE),
#'         new_campo("geracao", "float", FALSE)
#'     ),
#'     conexao = conect)
#' 
#' @return Objeto da classe \code{campo} contendo especificacao de um campo qualquer
#' 
#' @export

new_campo <- function(nome, tipo, foreignkey = FALSE, ref = NULL, camporef = NULL, proxy = NULL) {

    tipo <- switch(tipo,
        "inteiro" = "discreto",
        "string" = "discreto",
        "float" = "continuo",
        "data" = "data")

    if(is.null(tipo)) stop("'tipo' nao esta dentro dos valores permitidos -- Veja '?new_campo'")

    if(foreignkey) {
        if(is.null(ref)) stop("nao foi passado argumento 'ref' indicando tabela referencia")
        if(is.null(camporef)) stop("nao foi passado argumento 'camporef' indicando campo na 'ref'")
        if(is.null(proxy)) stop("nao foi passado argumento 'ref' indicando proxy na 'ref'")
    }

    out <- list(nome = nome)
    attr(out, "foreignkey") <- list(has = foreignkey, ref = ref, camporef = camporef, proxy = proxy)
    class(out) <- c("campo", paste0("campo_", tipo))

    return(out)
}

#' Generica Para Parse De Argumentos
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

parsearg.campo_discreto <- function(campo, valor, ...) {

    if(is.na(valor[1]) || valor[1] == "*") return(structure(NA, "n" = 0))

    nome <- campo$nome
    foreignkey <- attr(campo, "foreignkey")

    if(foreignkey[[1]]) {

        tabref <- foreignkey$ref
        camporef   <- foreignkey$camporef
        campoproxy <- foreignkey$proxy

        where_aux <- parsearg(tabref$campos[[campoproxy]], valor)
        select_aux <- camporef
        from_aux   <- tabref$nome
        query_aux <- list(SELECT = select_aux, FROM = from_aux, WHERE = where_aux)

        valor <- roda_query(attr(tabref, "conexao"), query_aux)[[camporef]]
    }

    if(class(valor[1]) == "character") {
        valor_str <- paste0(valor, collapse = "','")
        WHERE <- paste0(nome, " IN ('", valor, "')")
    } else {
        valor_str <- paste0(valor, collapse = ",")
        WHERE <- paste0(nome, " IN (", valor, ")")
    }
    attr(WHERE, "n") <- length(valor)

    return(WHERE)
}

#' @export

# implementacao provisoria
# ainda falta fazer os possiveis subsets por lista ou faixa
parsearg.campo_continuo <- function(campo, valor, ...) parsearg.campo_discreto(campo, valor, ...)

#' @export

parsearg.campo_data <- function(campo, valor, ...) {

    nome <- campo$nome

    if((valor[1] == "*") || is.na(valor[1])) valor <- "1000/3999"
    WHERE <- parsedatas(valor, nome)

    return(WHERE)
}
