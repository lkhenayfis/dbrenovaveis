.onLoad <- function(libname, pkgname) {
    cachedir <- file.path(Sys.getenv("HOME"), ".dbrenovaveis")
    Sys.setenv("dbrenovaveis-cachedir" = cachedir)

    if(!dir.exists(cachedir)) dir.create(cachedir)
}

.onUnload <- function(libname, pkgname) {
    Sys.unsetenv("dbrenovaveis-cachedir")
}