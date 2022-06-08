library(dplyr)
library(futile.logger)
library(data.table)
source("src/utils/inputs.R")
source("src/utils/ventas.R")
source("src/utils/configs.R")
source("src/utils/productos.R")
source("src/utils/pdvs.R")

pais <- 'br'

fuentes <- c('dia_semana_hora', 'mapa', 'totales', 'penetracao')
path_generico <- '../%s/src/%s.R'

correr_fuentes <- function(ventas, proveedor_objetivo, particiones_pdvs, particiones_productos, id_conf, geo, canal, mes){
  dir_salida <- file.path("datos", "salida", id_conf, mes)
  dir.create(dir_salida, showWarnings = F, recursive = T)
  canal_normalizado <- stringr::str_replace_all(canal, "\\+", "mas") %>% 
    stringr::str_replace_all(., " ", "_")
  geo_normalizado <- stringr::str_replace_all(geo, " ", "_") 
    
  lapply(fuentes, function(fuente) {
    script_cargar <- sprintf(path_generico, fuente, fuente)
    source(script_cargar)
    #procesar ventas
    flog.info("Ejecutando %s", fuente)
    resultado <- get(fuente)(ventas, proveedor_objetivo, particiones_pdvs, particiones_productos)
    fwrite(resultado, sprintf("%s/%s_%s_%s.csv", dir_salida, fuente, geo_normalizado, canal_normalizado))
  })
}


fechas <- NA
mes <- 202202
config_pais <- obtener_configuracion("br")
lista_empresas <- unique(config_pais$pemp_codigo)

pdvs_totales <- lapply(lista_empresas, function(p){
  Dimensiones::ObtenerPdvs2(pais, p, paste0(expandir_comodines(pais, c("geo", "canal")), collapse = ','))
}) %>% 
  rbindlist() %>% 
  unique()

lista_pdvs_totales <- pdvs_totales %>%
  group_split(across(c("geo", "canal")))

lapply(lista_pdvs_totales, function(p){
  geo <- p$geo[1]
  canal <- p$canal[1]
  flog.info("Extrayendo ventas para %s, %s", geo, canal)
  ventas <- obtener_ventas(pais, fechas, pdvs=pull(p, pdv_codigo))
  flog.info("Ventas extraidas para %s, %s", geo, canal)
  
  
  lapply(1:nrow(config_pais), function(i){
    config <- slice(config_pais, i)
    pais <- config$pais
    pemp_codigo <- config$pemp_codigo
    proveedor_objetivo <- config$proveedor_objetivo
    particiones_pdvs <- strsplit(config$particiones_pdvs, ", ")[[1]]
    particiones_productos <- strsplit(config$particiones_productos, ", ")[[1]]
    
    particiones_pdvs_limpio <- tolower(gsub(' ', '', gsub('"', '', particiones_pdvs))) %>% 
      stringi::stri_trans_general(., "latin-ascii")
    
    particiones_productos_limpio <- tolower(gsub(' ', '', gsub('"', '', particiones_productos))) %>% 
      stringi::stri_trans_general(., "latin-ascii")
    
    productos_emp <- obtener_productos(pais, pemp_codigo, particiones_productos)
    pdvs_emp <- obtener_pdvs(pais, pemp_codigo, particiones_pdvs)
    
    flog.info("Joineando ventas para %s, %s, %s", config$id, geo, canal)
    ventas_emp <- ventas %>%
      .[pdvs_emp , on="pdv_codigo", nomatch = 0 ] %>% 
      productos_emp[., on ="prod_codigo"]
    
    flog.info("Corriendo fuentes para %s, %s, %s", config$id, geo, canal)
    correr_fuentes(
      ventas_emp, proveedor_objetivo, particiones_pdvs_limpio, particiones_productos_limpio, config$id, geo, canal, mes)
  })
  gc()
})







