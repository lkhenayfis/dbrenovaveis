% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/querydados.r
\name{expandedatahora}
\alias{expandedatahora}
\title{Interpreta As Datahoras Passadas Em \code{datahora}}
\usage{
expandedatahora(datahora)
}
\arguments{
\item{datahora}{uma string indicando faixa de tempo simples, isto e, sem uso da contra barra,
como descrito em \code{\link{get_funs_quant}}}
}
\value{
vetor de duas strings, o inicio e final da janela representada por \code{data} hora com
intervalos inclusivo e exclusivo, respectivamente
}
\description{
Transforma as expressoes \code{datahoras} em limites POSIX correspondentes a janela
}
\details{
Como descrito em \code{\link{get_funs_quant}}, existem diversas maneiras de especificar uma janela de
tempo para acesso ao banco de dados. Esta funcao e responsavel por processar as strings de janela
temporal passadas la.

Antes de detalhar o processamento, e util apresentar a saida. \code{datahora} pode ser ou um
unico valor ou dois separados pela barra, mas de qualquer forma isto representa uma janela com
comeco e fim. \code{expandedatahora} retorna um vetor contendo as datahoras correspondentes a
estes limites, sendo o limite final exclusivo, isto e, retorna limites para uma busca do tipo
\eqn{x \in [lim_1, lim_2)}.

Se \code{datahora} for um valor temporal completo, contendo ate os minutos, entende-se que o
usuario esta buscando exatamente aquele registro e nao uma janela. Neste caso o vetor sera
retornado com \code{datahora} na primeira posicao e \code{datahora} mais um segundo na segunda.
Desta forma a busca executada e equivalente a uma busca \eqn{x == datahora}

Quando \code{datahora} e um valor simples incompleto, indicando entao uma janela, a expansao
depende do nivel de detalhe informado. Quando se informa apenas ano, mes ou dia, o vetor
retornado contera este ano, mes, ou dia no primeiro instante de tempo possivel na primeira
posiacao e, na segunda, o mesmo instante somado de um ano, mes ou dia respectivamente. Veja os
Exemplos para mais detalhes.
}
\examples{


\dontrun{
faixa <- expandedatahora("2021")
identical(faixa, c("2021-01-01 00:00:00", "2022-01-01 00:00:00"))
}

\dontrun{
faixa <- expandedatahora("2020-05-23 12:30")
identical(faixa, c("2020-05-23 12:30:00", "2020-05-23 12:30:01"))
}

}
