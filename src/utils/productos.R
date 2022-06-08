library(data.table)
obtener_productos <- function(pais, pemp_codigo, particiones_productos){
  Dimensiones::ObtenerProductos2(pais, pemp_codigo, proyeccion = paste0(particiones_productos, collapse = ',')) %>% 
    as.data.table()
}
