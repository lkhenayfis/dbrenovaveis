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
#' AINDA NAO IMPLEMENTADO
#' E possivel incluir particionamento nestes dados, de uma certa forma. Ao inves de uma tabela unica
#' de verificados, por exemplo, podem ser criadas n tabelas nomeadas \code{verificados_partI}, onde 
#' I corresponde a um indice numerico da particao. Nestes casos, deve existir uma nova tabela 
#' chamada \code{partitions_verificados} indicando o indice da particao na primeira coluna e valor
#' das colunas de particionamento nas restantes. Atualmente apenas particionamentos categoricos
#' sao suportados, isto e, uma faixa de datas por exemplo nao funciona como particionamento.
#' 
#' @param diretorio diretorio contendo os arquivos csv representando o banco
#' 
#' @return objeto de conexao com o arquivamento local
#' 
#' @export

conectalocal <- function(diretorio) {

    out <- diretorio
    class(out) <- "local"

    return(out)
}