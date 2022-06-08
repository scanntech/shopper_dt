obtener_ventas <- function(pais, fechas, pdvs = c(7698, 7691, 7682, 7657, 14146)) {
  ventas <-
    hechos::f_obtener_ventas_detalladas_por_pdvs(
      pais = 'br',
      fechas = seq(as.Date('2022-02-01'), as.Date('2022-02-28'), by = '1 days'),
      pdvs = pdvs,
      incluir_venta_codigo_interno = F,
      columns_ = c("fecha_comercial", "pdv_codigo", "prod_codigo", "hora", "cant_vta", "imp_vta", "id_ticket")
    )
  ventas
}
