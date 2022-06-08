library(dplyr)
library(futile.logger)
source("src/utils/inputs.R")
source("src/utils/ventas.R")


fuentes <- c('dia_semana_hora', 'mapa', 'totales', 'penetracao')
path_generico <- '../%s/src/%s.R'

#Obtener configuraciones de clientes activos.
# configs_ejecutar <- obtener_configs_shopper_activos(pais)
#para cada cliente, obtener particiones pdvs y particiones productos

#Parametros que van a salir de configs
pais <- 'br'
pemp_codigo <- 3
proveedor_objetivo <- 'AMBEV'
fechas <- NA
particiones_pdvs <- c('canal', 'geo')
particiones_productos <- c('"Categoria"', '"Proveedor"')

correr_fuentes <- function(ventas, proveedor_objetivo, particiones_pdvs, particiones_productos){
  lapply(fuentes, function(fuente) {
    script_cargar <- sprintf(path_generico, fuente, fuente)
    source(script_cargar)
    #procesar ventas
    flog.info("Ejecutando %s", fuente)
    resultado <- get(fuente)(ventas, proveedor_objetivo, particiones_pdvs, particiones_productos)
    fwrite(resultado, sprintf("salida_%s", fuente))
  })
}

particiones_pdvs_limpio <- tolower(gsub(' ', '', gsub('"', '', particiones_pdvs))) %>% 
  stringi::stri_trans_general(., "latin-ascii")

particiones_productos_limpio <- tolower(gsub(' ', '', gsub('"', '', particiones_productos))) %>% 
  stringi::stri_trans_general(., "latin-ascii")

productos <- Dimensiones::ObtenerProductos2(pais, pemp_codigo, proyeccion = paste0(particiones_productos, collapse = ','))
pdvs <- Dimensiones::ObtenerPdvs2(pais, pemp_codigo, paste0(expandir_comodines(pais, particiones_pdvs), collapse = ','))

lista_pdvs <- pdvs %>%
  group_split(across(particiones_pdvs))

lapply(lista_pdvs, function(p){
  flog.info("Obteniendo ventas para %s %s", p$canal[1], p$geo[1])

  ventas <- obtener_ventas(pais, fechas, pdvs=pull(p, pdv_codigo)) %>%
    .[pdvs, on="pdv_codigo", nomatch=0] %>%
    .[productos, on = "prod_codigo", nomatch=0]
  
  correr_fuentes(ventas, proveedor_objetivo, particiones_pdvs_limpio, particiones_productos_limpio)
  rm(ventas)
  gc()
})

