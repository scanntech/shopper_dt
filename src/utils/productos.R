library(data.table)
library(conexiones)
library(glue)
library(readr)
obtener_productos_dw <- function(pais, pemp_codigo, pemp_prueba, particiones){
  
  particiones <- setdiff(particiones, c("codigo_barras", "\"Categoria\"", "\"Proveedor\""))
  if (length(particiones>0)){
    string_particiones <- paste(",", paste(particiones, collapse = ","), sep="")
  } else{
    string_particiones <- ""
  }
  
  query <- glue(read_file("sql/productos_fuentes.sql"))
  
  conn <- conexion_dw(pais)
  prd <- dbGetQueryDT(conn, query)
  
  prd$prov_cat <- paste(prd$proveedor,"-",prd$categoria)
  prd$prov_cat_mar <- paste(prd$proveedor,"-",prd$categoria,"-",prd$marca)
  
  prd
  
}

obtener_barras <- function(pais){
  query <- 'SELECT CODIGO "prod_codigo", LTRIM(CODIGO_BARRAS, 0) "codigo_barras" FROM dw.d_producto'
  conn <- conexion_dw(pais)
  dbGetQueryDT(conn, query)
}

unificar_prod_codigo <- function(df){
  result <- df[order(-prod_codigo)][, by = "codigo_barras", .SD[1]]
  names(result) <- c("codigo_barras", "codigo_unico")
  result
}
