expandir_comodines <- function(pais, particiones_pdvs){

  if(any(grepl('geo', particiones_pdvs))){
    def_geo <- paste0(Dimensiones::ObtenerDefGeosDefault(pais), ' geo')
    particiones_pdvs <- gsub('geo', def_geo, particiones_pdvs)
  }

  if(any(grepl('canal', particiones_pdvs))){
    def_canal <- paste0(Dimensiones::ObtenerDefCanalesDefault(pais), ' canal')
    particiones_pdvs <- gsub('canal', def_canal, particiones_pdvs)
  }
  particiones_pdvs
}

