library(conexiones)
obtener_configuracion <- function(pais){
  dbGetQueryDT("algoritmo", sprintf("SELECT *  FROM configs_shopper_dt WHERE pais = '%s'", pais))
}
