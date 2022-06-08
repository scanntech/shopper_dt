  CREATE TABLE configs_shopper_dt (
  	id PRIMARY KEY,
  	pais VARCHAR (3) NOT NULL,
  	pemp_codigo INTEGER NOT NULL,
  	proveedor_objetivo text NOT NULL,
  	particiones_pdvs text NOT NULL,
  	particiones_productos text NOT NULL,
  	audit_user VARCHAR(64) NOT NULL,
  	audit_date TIMESTAMP NOT NULL
  )
