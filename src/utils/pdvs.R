library(data.table)
obtener_pdvs <- function(pais, pemp_codigo, particiones_pdvs){
  Dimensiones::ObtenerPdvs2(pais, pemp_codigo, paste0(expandir_comodines(pais, particiones_pdvs), collapse = ',')) %>% 
    as.data.table()
}
