library(dplyr)
library(futile.logger)
library(data.table)
library(testthat)
library(peakRAM)
source("src/utils/inputs.R")
source("src/utils/ventas.R")
source("src/utils/configs.R")
source("src/utils/productos.R")
source("src/utils/pdvs.R")
library(pryr)

pais <- 'br'

fuentes <- c('dia_semana_hora', 'mapa', 'totales', 'penetracao')
#fuentes <- c('mapa')

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
    #print(names(ventas))
    
    cols_ventas_antes <- colnames(ventas)
    cols_ventas_antes <- c(cols_ventas_antes, NULL)
    
    ventas_original <- copy(ventas)
    
    memoria_delta <- mem_change(
      memoria_p <- peakRAM(
        resultado <- get(fuente)(ventas, proveedor_objetivo, particiones_pdvs, particiones_productos)
      )
    )
    
    memoria_d<-data.table()
    memoria_d$memoria <- memoria_delta
    memoria_d$fuente <- fuente
    memoria_d$geo <- geo
    memoria_d$canal <- canal
    memoria_d$proveedor <- proveedor_objetivo
   
    memoria_p$fuente <- fuente
    memoria_p$geo <- geo
    memoria_p$canal <- canal
    memoria_p$proveedor <- proveedor_objetivo
    
    
    fwrite(memoria_p, sprintf("%s/%s_%s_%s_memoria_peak.csv", dir_salida, fuente, geo_normalizado, canal_normalizado))
    fwrite(memoria_d, sprintf("%s/%s_%s_%s_memoria_delta.csv", dir_salida, fuente, geo_normalizado, canal_normalizado))
    
    ventas <- ventas_original
    rm(ventas_original)
    
    cols_ventas_desp <- names(ventas)
    expect_equal(cols_ventas_antes, cols_ventas_desp)
    
    ruta_salida <- sprintf("%s/%s_%s_%s.csv", dir_salida, fuente, geo_normalizado, canal_normalizado)
    flog.info(paste("Escribiendo en", ruta_salida))
    fwrite(resultado, ruta_salida)
    
  })
}


fechas <- NA
mes <- 202202
#config_pais <- obtener_configuracion("br") %>% slice(1)
config_pais <- obtener_configuracion("br") 
lista_empresas <- unique(config_pais$pemp_codigo)

pdvs_totales <- lapply(lista_empresas, function(p){
  Dimensiones::ObtenerPdvs2(pais, p, paste0(expandir_comodines(pais, c("geo", "canal")), collapse = ','))
}) %>% 
  rbindlist() %>% 
  unique()

#pdvs_totales <- pdvs_totales %>% filter(geo=="PARANA" & canal=="5 a 9")

lista_pdvs_totales <- pdvs_totales %>%
  group_split(across(c("geo", "canal")))

#lista_pdvs_totales <- lista_pdvs_totales[1:4]



lapply(lista_pdvs_totales, function(p){
  geo <- p$geo[1]
  canal <- p$canal[1]
  
  flog.info("Extrayendo ventas para %s, %s", geo, canal)
  M1 <- mem_change(ventas <- obtener_ventas(pais, fechas, pdvs=pull(p, pdv_codigo)))
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
    M2 <- mem_change(ventas_emp <- ventas %>%
      .[pdvs_emp , on="pdv_codigo", nomatch = 0 ] %>% 
      productos_emp[., on ="prod_codigo"])
    
    flog.info("Corriendo fuentes para %s, %s, %s", config$id, geo, canal)
    M3 <- mem_change(correr_fuentes(
      ventas_emp, proveedor_objetivo, particiones_pdvs_limpio, particiones_productos_limpio, config$id, geo, canal, mes
      ))
    
    data.table(M1=M1, M2=M2, M3=M3, id=config$id, geo=geo, canal=canal) %>% 
      fwrite(sprintf("datos/salida/%s/%s/%s_%s_memoria.csv", config$id, mes, geo, canal))
  })
  gc()
  
})





