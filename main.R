library(dplyr)
library(lubridate)
library(futile.logger)
library(data.table)
library(testthat)
library(peakRAM)
source("src/utils/inputs.R")
source("src/utils/ventas.R")
source("src/utils/configs.R")
source("src/utils/productos.R")
source("src/utils/pdvs.R")
source("../convivencia_shopper/src/convivencia_shopper.R")
library(pryr)

correr_fuentes <- function(ventas, productos, pdvs, proveedor_objetivo, particiones_pdvs, particiones_productos, id_conf, geo, canal, mes){
  dir_salida <- file.path("datos", "salida", id_conf, mes)
  dir.create(dir_salida, showWarnings = F, recursive = T)
  canal_normalizado <- stringr::str_replace_all(canal, "\\+", "mas") %>% 
    stringr::str_replace_all(., " ", "_")
  geo_normalizado <- stringr::str_replace_all(geo, " ", "_") 
  
  columnas_conv <- unique(
    c(
      c("fecha_comercial", "prod_codigo", "pdv_codigo", "cant_vta", "imp_vta", "id_ticket", "codigo_barras", "categoria", "proveedor", "marca", "descripcion", "prov_cat", "prov_cat_mar"),
      particiones_productos,
      particiones_pdvs)
    )
  
  ventas_conv <- ventas %>%
    .[pdvs , on="pdv_codigo", nomatch = 0 ] %>% 
    productos[., on ="prod_codigo"] %>% 
    select(columnas_conv) %>% 
    na.omit()
  
  prd_conv <- productos[prod_codigo %in% ventas_conv$prod_codigo]
  
  convivencia <- convivencia_shopper(ventas_conv, proveedor_objetivo, prd_conv)
  
  rm(ventas_conv)
  rm(prd_conv)
  
  columnas_fuentes <- unique(
    c(
      c("fecha_comercial", "prod_codigo", "pdv_codigo", "cant_vta", "imp_vta", "id_ticket", "hora", "categoria", "proveedor"),
      particiones_productos_limpio,
      particiones_pdvs_limpio)
    )
  
  ventas_fuentes <- ventas %>%
    .[pdvs , on="pdv_codigo", nomatch = 0 ] %>% 
    productos[., on ="prod_codigo"] %>% 
    select(columnas_fuentes)
  
  lapply(fuentes, function(fuente) {
    script_cargar <- sprintf(path_generico, fuente, fuente)
    source(script_cargar)
    #procesar ventas
    flog.info("Ejecutando %s", fuente)

    memoria_delta <- mem_change(
      memoria_p <- peakRAM(
        resultado <- get(fuente)(ventas_fuentes, proveedor_objetivo, particiones_pdvs, particiones_productos)
      )
    )

    # memoria_d<-data.table()
    # memoria_d$memoria <- memoria_delta
    # memoria_d$fuente <- fuente
    # memoria_d$geo <- geo
    # memoria_d$canal <- canal
    # memoria_d$proveedor <- proveedor_objetivo
    # 
    # memoria_p$fuente <- fuente
    # memoria_p$geo <- geo
    # memoria_p$canal <- canal
    # memoria_p$proveedor <- proveedor_objetivo


    #fwrite(memoria_p, sprintf("%s/%s_%s_%s_memoria_peak.csv", dir_salida, fuente, geo_normalizado, canal_normalizado))
    #fwrite(memoria_d, sprintf("%s/%s_%s_%s_memoria_delta.csv", dir_salida, fuente, geo_normalizado, canal_normalizado))

    ruta_salida <- sprintf("%s/%s_%s_%s.csv", dir_salida, fuente, geo_normalizado, canal_normalizado)
    flog.info(paste("Escribiendo en", ruta_salida))
    fwrite(resultado, ruta_salida)

   })
  fuentes_c = c('skus','marcas', 'proveedor', 'categorias')
  lapply(fuentes_c, function(f){
    ruta_salida <- sprintf("%s/convivencia_%s_%s_%s.csv", dir_salida, f, geo_normalizado, canal_normalizado)
    fwrite(convivencia[[f]], ruta_salida)
  })
}


fechas <- NA
mes <- 202205
pais <- 'br'
fuentes <- c('dia_semana_hora', 'mapa', 'totales', 'penetracao')
path_generico <- '../%s/src/%s.R'

#Esto se va
config_pais <- obtener_configuracion(pais)[4]
geo <- Dimensiones::ObtenerGrillaGeoCanalDefault(lista = T)[[1]]$geo
canal <- Dimensiones::ObtenerGrillaGeoCanalDefault(lista = T)[[1]]$canal

flog.info(sprintf("Obteniendo codigos de barra para: %s", pais))
codigos_de_barra <- obtener_barras(pais)
flog.info(sprintf("Obtenidos codigos de barra para: %s", pais))

lapply(Dimensiones::ObtenerGrillaGeoCanalDefault(lista = T), function(r){
  geo <- r$geo[1]
  canal <- r$canal[1]
  
  flog.info("Extrayendo ventas para %s, %s", geo, canal)
  M1 <- mem_change(
    ventas <- 
      hechos::f_obtener_ventas_detalladas_por_geo_canal(geo, canal, pais, mes)
      )
  unificar_codigos_ventas(ventas, codigos_de_barra[prod_codigo %in% ventas$prod_codigo])
  flog.info("Ventas extraidas para %s, %s", geo, canal)
  

  
  lapply(1:nrow(config_pais), function(i){
    config <- slice(config_pais, i)
    pemp_codigo <- config$pemp_codigo
    proveedor_objetivo <- config$proveedor_objetivo
    particiones_pdvs <- strsplit(config$particiones_pdvs, ", ")[[1]]
    particiones_productos <- strsplit(config$particiones_productos, ", ")[[1]]
    
    particiones_pdvs_limpio <- tolower(gsub(' ', '', gsub('"', '', particiones_pdvs))) %>% 
      stringi::stri_trans_general(., "latin-ascii")
    
    particiones_productos_limpio <- tolower(gsub(' ', '', gsub('"', '', particiones_productos))) %>% 
      stringi::stri_trans_general(., "latin-ascii")
    
    productos <- obtener_productos_dw(pais, pemp_codigo, Dimensiones::EmpresaPrueba(pais), particiones_productos)
    
    pdvs <- obtener_pdvs(pais, pemp_codigo, particiones_pdvs)
    
    flog.info("Corriendo fuentes para %s, %s, %s", config$id, geo, canal)
    M3 <- mem_change(correr_fuentes(
      ventas, productos, pdvs ,proveedor_objetivo, particiones_pdvs_limpio, particiones_productos_limpio, config$id, geo, canal, mes
      ))
    
    #data.table(M1=M1, M2=M2, M3=M3, id=config$id, geo=geo, canal=canal) %>% 
     # fwrite(sprintf("datos/salida/%s/%s/%s_%s_memoria.csv", config$id, mes, geo, canal))
  })
  gc()
  
})



