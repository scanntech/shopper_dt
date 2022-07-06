SELECT dp.CODIGO "prod_codigo",
MARCA_DESCRIPCION "marca",
DESCRIPCION "descripcion",
LTRIM(CODIGO_BARRAS, 0) "codigo_barras", "Proveedor",
CASE 
  WHEN dpe."Categoria" IS NULL THEN dpp."Categoria" ELSE dpe."Categoria"
END "Categoria" {string_particiones}
FROM dwcla.D_PRODUCTO_{pemp_prueba} dpp 
LEFT JOIN DWCLA.D_PRODUCTO_{pemp_codigo} dpe ON dpe.CODIGO_SE = dpp.CODIGO_SE 
JOIN DW.D_PRODUCTO dp ON dpp.CODIGO_SE = dp.CODIGO 
JOIN dwcla.D_PRODUCTO_MODELO dpm ON dpp.CODIGO_SE = dpm.CODIGO_SM