################################# FUNCOES GERAIS USADAS NO PACOTE ##################################

#' Conexao Com Bancos
#' 
#' Gera a conexao com o banco \code{banco} utilizando as credenciais de \code{usuario}
#' 
#' Esta funcao e um simples wrapper para facilitacao da conexao com bancos de dados, utilizando
#' as credenciais previamente registradas e salvas pelas funcoes apropriadas. 
#' 
#' @param usuario a tag associada a credenciais previamente regristradas com
#'     \code{\link{registra_credenciais}}
#' @param banco a tag associada a informacoes de banco previamente regristradas com
#'     \code{\link{registra_banco}}
#' 
#' @examples 
#' 
#' # supondo usuario 'user' e banco 'db' ja registrados
#' 
#' \dontrun{
#' # conecta ao banco com credenciais de 'user'
#' conn <- conectabanco("user", "db")
#' 
#' # listando todas as tabelas no banco
#' dbListTables(conn)
#' 
#' # listando campos (colunas) de uma determinada tabela
#' dbListFields(conn, "nome_da_tabela")
#' }
#' 
#' @return objeto de conexao ao banco. Veja \code{\link[DBI]{dbConnect}} para mais detalhes
#' 
#' @import DBI RPostgreSQL
#' 
#' @export

conectabanco <- function(usuario, banco) {
    usuario <- readRDS(file.path(Sys.getenv("dbrenovaveis-cachedir"), paste0("user_", usuario, ".rds")))
    banco   <- readRDS(file.path(Sys.getenv("dbrenovaveis-cachedir"), paste0("db_", banco, ".rds")))

    conn <- dbConnect(
        drv = dbDriver("PostgreSQL"),
        user = usuario[[1]], password = usuario[[2]],
        host = banco[[1]], port = banco[[2]], dbname = banco[[3]]
    )

    return(conn)
}

#' Conexao Com Arquivos Locais
#' 
#' Gera a conexao com um mock banco, correspondente a um diretorio local com arquivos a ler
#' 
#' Os arquivamentos locais suportados por \code{dbrenovaveis} correspondem a diretorios contendo
#' uma serie de arquivos csv correspondentes as tabelas do banco (algo como um dump de um banco
#' relacional comum). Os arquivos devem ser nomeados tal qual os nomes de tabelas esperados no banco
#' assim como os nomes e tipos de dado nas colunas de cada um.
#' 
#' Particionamento de tabelas e suportado, de forma restrita. Inicialmente, para bancos locais 
#' contendo uma ou mais tabelas particionadas, deve existir um arquivo .PARTICAO.json indicando 
#' quais tabelas sao particionadas. Este arquivo tem estrutura muito simples, uma lista de apenas um
#' nivel com valores booleanos (ver o exemplo em \code{extdata/compart}).
#' 
#' Os dados particionados devem seguir uma estrutura predefinida. Particoes de uma determinada
#' tabela devem ser nomeadas \code{tabela_partA_partB_...} em que \code{partX} sao chaves 
#' indicadoras apontando os multiplos niveis de particionamento. Por exemplo, particionando-se a 
#' tabela de previstos por usina e modelo, teria-se: previstos_1_1, previstos_2_1, ... previstos_N_1, 
#' previstos_1_2, ..., previstos_N_M, onde N e o maximo id de usinas e M o de modelos.
#' 
#' Nestes casos, a tabela original (previstos, no exemplo acima) deve ser uma tabela mestra contendo
#' a associacao entre subtabelas e a particao a que correspondem. As tabelas mestras devem conter um
#' coluna chamada \code{tabela}, e as demais devem ser as colunas pelas quais suas particoes sao
#' separadas. Voltando ao exemplo de previstos
#' 
#' | tabela | id_usina | id_modelo |
#' | ---- | ---- | ---- |
#' | previstos_1_1 | 1 | 1 |
#' | previstos_2_1 | 2 | 1 |
#' | previstos_1_2 | 1 | 2 |
#' 
#' A ordem das colunas nao importa, bem como o numero de particoes e colunas pelas quais se 
#' particiona nao precisam necessariamente ser estas do exemplo. As tabelas de particao devem ter
#' \bold{EXATAMENTE A MESMA ESTRUTURA} da equivalente sem particao, isto e, devem manter as colunas
#' pelas quais foram originalmente particionadas.
#' 
#' Atualmente apenas particionamentos categoricos sao suportados, isto e, uma faixa de datas nao
#' funciona como particionamento.
#' 
#' @param diretorio diretorio contendo os arquivos csv representando o banco
#' 
#' @return objeto de conexao com o arquivamento local
#' 
#' @export

conectalocal <- function(diretorio) {

    partfile   <- file.path(diretorio, ".PARTICAO.json")
    tempartdict <- file.exists(partfile)

    if(tempartdict) {
        particoes <- unlist(jsonlite::read_json(partfile))
    } else {
        particoes <- list.files(diretorio)
        particoes <- sapply(particoes, function(s) sub(paste0(".", tools::file_ext(s)), "", s))
        particoes <- structure(rep(FALSE, length(particoes)), names = particoes)
    }

    out <- diretorio
    attr(out, "extensao") <- paste0(".", tools::file_ext(list.files(diretorio)[1]))
    attr(out, "particoes") <- particoes
    class(out) <- "local"

    return(out)
}
