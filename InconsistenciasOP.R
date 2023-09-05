# Inconsistencias de ocupación principal 
library(conflicted)
conflicted::conflict_prefer('filter','dplyr')

library(haven)
library(dplyr)
library(readxl)
library(writexl)

library(RMySQL)

rm(list = ls())
cat('\014')

ruta <- "Mario"
grupos <- "Grupos.xlsx"

sql <- 
"
SELECT encuestador ENCUESTADOR, depto DEPTO, mupio MUPIO, sector SECTOR, estructura ESTRUCTURA, vivienda VIVIENDA, hogar HOGAR, p.cp CP, 
	10 CAPITULO, 'C' SECCION, 2 PREGUNTA,
	'P10C02 Ocupación principal falta o insuficientemente descrita' AS 'DEFINICION DE INCONSISTENCIA',
	'10C00251' AS 'CODIGO ERROR', '' COMENTARIOS,
	p.p10c02 VALOR
FROM `level-1` l
	INNER JOIN cases c ON c.id=l.`case-id`
	INNER JOIN personas p ON p.`level-1-id` = l.`level-1-id` 
	INNER JOIN caratula r ON r.`level-1-id`=l.`level-1-id`
WHERE c.deleted=0 AND p.p10c01>=1 AND r.estado_pr=1 AND (LENGTH(IFNULL(p.p10c02,''))<=3 OR UPPER(IFNULL(p.p10c02,'')) IN ('NO','NA','N/A','NADA','ESTUDIA','ESTUDIAR','NINGUNO') ) AND 
	DATEDIFF(CURDATE(),(SELECT STR_TO_DATE(MAX(v.r1_fecha_inicial),'%d/%m/%y') FROM registro_de_visitas_pr v WHERE v.`level-1-id`=l.`level-1-id`))<=4
UNION
SELECT encuestador ENCUESTADOR, depto DEPTO, mupio MUPIO, sector SECTOR, estructura ESTRUCTURA, vivienda VIVIENDA, hogar HOGAR, p.cp CP, 
	10 CAPITULO, 'C' SECCION, 3 PREGUNTA,
	'P10C03 Actividad principal falta o insuficientemente descrita' AS 'DEFINICION DE INCONSISTENCIA',
	'10C00351' AS 'CODIGO ERROR', '' COMENTARIOS,
	p.p10c03 VALOR
FROM `level-1` l
	INNER JOIN cases c ON c.id=l.`case-id`
	INNER JOIN personas p ON p.`level-1-id` = l.`level-1-id` 
	INNER JOIN caratula r ON r.`level-1-id`=l.`level-1-id`
WHERE c.deleted=0 AND p.p10c01>=1 AND r.estado_pr=1 AND (LENGTH(IFNULL(p.p10c03,''))<=3 OR UPPER(IFNULL(p.p10c03,'')) IN ('NO','NA','N/A','NADA','ESTUDIA','ESTUDIAR','NINGUNA','NINGUNO') ) AND 
	DATEDIFF(CURDATE(),(SELECT STR_TO_DATE(MAX(v.r1_fecha_inicial),'%d/%m/%y') FROM registro_de_visitas_pr v WHERE v.`level-1-id`=l.`level-1-id`))<=4
UNION
SELECT encuestador ENCUESTADOR, depto DEPTO, mupio MUPIO, sector SECTOR, estructura ESTRUCTURA, vivienda VIVIENDA, hogar HOGAR, p.cp CP, 
	10 CAPITULO, 'C' SECCION, 4 PREGUNTA,
	'P10C04 Empresa falta o insuficientemente descrita' AS 'DEFINICION DE INCONSISTENCIA',
	'10C00451' AS 'CODIGO ERROR', '' COMENTARIOS,
	p.p10c04 VALOR
FROM `level-1` l
	INNER JOIN cases c ON c.id=l.`case-id`
	INNER JOIN personas p ON p.`level-1-id` = l.`level-1-id` 
	INNER JOIN caratula r ON r.`level-1-id`=l.`level-1-id`
WHERE c.deleted=0 AND p.p10c01>=1 AND r.estado_pr=1 AND (LENGTH(IFNULL(p.p10c04,''))<=2 OR UPPER(IFNULL(p.p10c04,'')) IN ('NO','NA','N/A','POR','NADA','ESTUDIA','ESTUDIAR') ) AND 
	DATEDIFF(CURDATE(),(SELECT STR_TO_DATE(MAX(v.r1_fecha_inicial),'%d/%m/%y') FROM registro_de_visitas_pr v WHERE v.`level-1-id`=l.`level-1-id`))<=4
UNION
SELECT encuestador ENCUESTADOR, depto DEPTO, mupio MUPIO, sector SECTOR, estructura ESTRUCTURA, vivienda VIVIENDA, hogar HOGAR, p.cp CP, 
	10 CAPITULO, 'C' SECCION, 7 PREGUNTA,
	'P10C07 Productos falta o insuficientemente descrita' AS 'DEFINICION DE INCONSISTENCIA',
	'10C00751' AS 'CODIGO ERROR', '' COMENTARIOS,
	p.p10c07 VALOR
FROM `level-1` l
	INNER JOIN cases c ON c.id=l.`case-id`
	INNER JOIN personas p ON p.`level-1-id` = l.`level-1-id` 
	INNER JOIN caratula r ON r.`level-1-id`=l.`level-1-id`
WHERE c.deleted=0 AND p.p10c01>=1 AND r.estado_pr=1 AND (LENGTH(IFNULL(p.p10c07,''))<=2 OR UPPER(IFNULL(p.p10c07,'')) IN ('NO','NA','N/A','POR','NADA','ESTUDIA','ESTUDIAR') ) AND 
	DATEDIFF(CURDATE(),(SELECT STR_TO_DATE(MAX(v.r1_fecha_inicial),'%d/%m/%y') FROM registro_de_visitas_pr v WHERE v.`level-1-id`=l.`level-1-id`))<=4;
"

data <- NULL
ex <- tryCatch({
  cnn <- dbConnect(MySQL(), 
                   user = 'mchinchilla', 
                   password = 'mchinchilla$2023', 
                   dbname = 'ENCOVI_PR', 
                   host = '20.10.8.4', 
                   port=3307)
  
  #Obtener datos de ocupaciones
  data <- dbGetQuery(cnn, sql)
  
}, finally = {
  dbDisconnect(cnn)
})

if (nrow(data) >= 1) {
  gs <- read_excel(file.path(ruta, grupos))
  data <- data %>% 
    left_join(gs, by = c("DEPTO", "MUPIO", "SECTOR"))

  message(paste0("Total de inconsistencias: ", nrow(data)))
  
  # Crear la carpeta "Mario" si no existe
  if (!dir.exists("Mario")) {
    dir.create("Mario")
  }
  
  # Crear una carpeta con marca temporal
  timestamp <- format(Sys.time(), "%d-%m-%H-%M")
  timestamp_folder <- paste0("Mario/Inconsistencias_", timestamp)
  dir.create(timestamp_folder)
  
  # Guardar el archivo Excel de inconsistencias totales
  write_xlsx(data, file.path(timestamp_folder, "Inconsistencias.xlsx"))
  
  lista <- unique(data$GRUPO)
  for (item in lista) {
    cuadro <- data %>% 
      filter(GRUPO == item) %>%
      select(-c("VALOR", "GRUPO"))
    
    nombre <- paste0("InconsistenciasGRUPO", item, ".xlsx")
    
    message(paste0(">> ", nombre, " -> ", nrow(cuadro)))
    
    # Guardar el archivo Excel de inconsistencias por grupo
    write_xlsx(cuadro, file.path(timestamp_folder, nombre))
  }
  
} else {
  message("No se recuperó información")
}

