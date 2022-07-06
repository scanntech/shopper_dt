obtener_ventas <- function(pais, fechas, pdvs = c(7698, 7691, 7682, 7657, 14146)) {
  ventas <-
    hechos::f_obtener_ventas_detalladas_por_pdvs(
      pais = 'br',
      fechas = seq(as.Date('2022-05-01'), as.Date('2022-05-30'), by = '1 days'),
      pdvs = pdvs,
      incluir_venta_codigo_interno = F,
      columns_ = c("fecha_comercial", "pdv_codigo", "prod_codigo", "hora", "cant_vta", "imp_vta", "id_ticket")
    )
  ventas
}

unificar_codigos_ventas <- function(ventas, codigos_de_barra){
  flog.info("Unificando")
  ventas <- ventas[codigos_de_barra, on="prod_codigo"]
  ventas <- ventas[unificar_prod_codigo(codigos_de_barra), on="codigo_barras"]
  ventas$nro_ticket <- ventas$id_ticket %>%
    stringr::str_split("-") %>% 
    lapply(function(s){
      s[2]
    }) %>% 
    unlist()
  ventas$id_ticket <- paste(ventas$codigo_unico, ventas$nro_ticket, sep="-")
  ventas$prod_codigo <- ventas$codigo_unico
  ventas %>%
    group_by(codigo_barras, fecha_comercial, pdv_codigo, prod_codigo, id_ticket) %>% 
    summarise(
      cant_vta = sum(cant_vta),
      imp_vta = sum(imp_vta),
      hora = first(hora),
      .groups = 'drop') %>% 
    as.data.table()
}

