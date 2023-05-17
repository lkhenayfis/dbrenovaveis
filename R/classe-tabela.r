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
