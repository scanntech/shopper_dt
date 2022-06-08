library(data.table)
library(dplyr)
library(Dimensiones)
library(futile.logger)
source('src/utils/ventas.R')
source('src/utils/inputs.R')
source('src/dia_semana_hora.R')

##### Parametros #####
pais <- 'br'
pemp_codigo <- 3
proveedor_objetivo <- 'AMBEV'
fechas <- NA
particiones_pdvs <- c('canal', 'geo')
particiones_productos_sql <- c('"Segmento" "categoria_empresa"', '"Categoria" "categoria_prueba"', '"Proveedor"')
particiones_productos <- c('"Categoria"', '"Proveedor"')

filtros <- c()


##### Datos ######


#TODO ver de no sacar todas las ventas, solo la de los prods y pdvs que necesitamos

productos <- ObtenerProductos2(pais, pemp_codigo, proyeccion = paste0(particiones_productos, collapse = ','))

pdvs <- ObtenerPdvs2(pais, pemp_codigo, paste0(expandir_comodines(pais, particiones_pdvs), collapse = ','))

particiones_pdvs <- tolower(gsub(' ', '', gsub('"', '', particiones_pdvs)))
particiones_pdvs <- stringi::stri_trans_general(particiones_pdvs, "latin-ascii")

lista_pdvs <- pdvs %>%
  group_split(across(particiones_pdvs))

particiones_productos <- tolower(gsub(' ', '', gsub('"', '', particiones_productos)))
particiones_productos <- stringi::stri_trans_general(particiones_productos, "latin-ascii")

result <- rbindlist(lapply(lista_pdvs, function(p){
  flog.info("Obteniendo ventas para %s %s", p$canal[1], p$geo[1])
  #leftjoinear a posteriori
  ventas <- obtener_ventas(pais, fechas, pdvs=pull(p, pdv_codigo)) %>%
    .[pdvs, on="pdv_codigo", nomatch=0] %>%
    .[productos, on = "prod_codigo", nomatch=0]
  flog.info("Ejecutando dia_semana_hora para %s %s", p$canal[1], p$geo[1])
  dia_semana_hora(ventas, proveedor_objetivo, particiones_pdvs, particiones_productos)
  rm(ventas)
  gc()
}))

fwrite(result, "salida1.csv")




