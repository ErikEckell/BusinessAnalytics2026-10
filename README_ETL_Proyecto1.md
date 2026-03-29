# Proyecto 1 BI - Guia de uso con tus CSV

## 1) Ejecutar ETL
En la carpeta del proyecto, ejecuta:

```powershell
BA2026-10/.venv/Scripts/python.exe cleancsv.py
```

Nota: el nombre real del archivo de festivos tiene acentos. Si copias y pegas el comando opcional, ajusta el nombre para que coincida exactamente con el archivo.

## 2) Archivos de salida esperados
El ETL genera una carpeta `output` con:

- `fact_crimes_enriched.csv`
- `agg_annual.csv`
- `agg_monthly.csv`
- `agg_weekly.csv`
- `agg_daily.csv`
- `agg_hourly.csv`
- `agg_15min.csv`
- `kpi_overview.csv`
- `kpi_top_crimes_by_year.csv`
- `kpi_victim_sex_distribution.csv`
- `kpi_case_status_distribution.csv`
- `kpi_area_distribution.csv`
- `kpi_victim_descent_distribution.csv`
- `kpi_weapon_type_distribution.csv`
- `kpi_sex_age_distribution.csv`

## 3) Que limpieza y enriquecimiento aplica el script

- Parseo de fechas de ocurrencia y reporte.
- Normalizacion de hora del delito desde `TIME OCC`.
- Derivacion de campos de tiempo para analisis:
  - anio, mes, semana ISO, dia, hora, tramo de 15 minutos.
- Enriquecimiento con calendario de festivos:
  - `is_holiday`
  - `holiday_name`
- Estandarizacion de valores vacios en sexo, descendencia, estado de caso, descripcion de arma y descripcion de delito.
- Limpieza basica de edad de victima (descarta edades fuera de rango).

## 4) Vistas minimas pedidas por la pauta
Con los CSV agregados ya tienes directamente:

- Vista anual: `agg_annual.csv`
- Vista mensual: `agg_monthly.csv`
- Vista semanal: `agg_weekly.csv`
- Vista diaria: `agg_daily.csv`
- Vista horaria: `agg_hourly.csv`
- Vista cada 15 minutos: `agg_15min.csv`

## 5) Propuesta de KPI (base)

1. Evolucion anual de delitos.
  - Fuente: `agg_annual.csv`.
2. Evolucion mensual de delitos.
  - Fuente: `agg_monthly.csv`.
3. Evolucion semanal de delitos.
  - Fuente: `agg_weekly.csv`.
4. Evolucion diaria de delitos.
  - Fuente: `agg_daily.csv`.
5. Porcentaje de delitos con arma.
  - Fuente: `agg_annual.csv`, `agg_monthly.csv` (`pct_with_weapon`).
6. Porcentaje de delitos en festivos.
  - Fuente: `agg_annual.csv`, `agg_monthly.csv` (`pct_holiday`).
7. Codigo de origen de victimas con mayor cantidad de delitos.
  - Fuente: `kpi_victim_descent_distribution.csv`.
8. Cantidad de delitos segun tipo de arma (Matias Kupfer).
  - Fuente: `kpi_weapon_type_distribution.csv`.
9. Cantidad de delitos segun sexo y edad (Victor Lozano).
  - Fuente: `kpi_sex_age_distribution.csv`.

## 5.1) Como formar los 9 KPI en Power BI (paso a paso)

1. Carga solo archivos desde `output`.
2. Para KPIs de tendencia usa `agg_annual.csv`, `agg_monthly.csv`, `agg_weekly.csv`, `agg_daily.csv`.
3. Para KPIs especificos de perfil usa `kpi_victim_descent_distribution.csv`, `kpi_weapon_type_distribution.csv`, `kpi_sex_age_distribution.csv`.
4. Para filtros globales (fecha, area, tipo de delito) usa `fact_crimes_enriched.csv`.

KPI 1. Incidentes totales
- Tabla: `kpi_overview.csv`
- Filtro: metric = `total_incidents`
- Visual: Tarjeta

KPI 2. Variacion porcentual de delitos (mensual)
- Tabla: `agg_monthly.csv`
- Visual: Linea por `year` + `month`
- Medida DAX sugerida:

```DAX
Incidentes = SUM(agg_monthly[incidents])

Incidentes Mes Anterior =
CALCULATE(
    [Incidentes],
    DATEADD('Calendar'[Date], -1, MONTH)
)

Variacion % Mes =
DIVIDE([Incidentes] - [Incidentes Mes Anterior], [Incidentes Mes Anterior])
```

KPI 3. Porcentaje de delitos en festivos
- Tabla: `agg_monthly.csv` o `agg_annual.csv`
- Campo: `pct_holiday`
- Visual: Tarjeta + linea de tendencia

KPI 4. Porcentaje de delitos con arma
- Tabla: `agg_monthly.csv` o `agg_annual.csv`
- Campo: `pct_with_weapon`
- Visual: Tarjeta + linea de tendencia

KPI 5. Hora pico de delitos
- Tabla: `agg_hourly.csv`
- Campo: `hour`, `incidents`
- Visual: Barras

KPI 6. Franja critica de 15 minutos
- Tabla: `agg_15min.csv`
- Campo: `time_15m`, `incidents`
- Visual: Barras ordenadas desc por `incidents`

KPI 7. Codigo de origen de victimas mas afectadas
- Tabla: `kpi_victim_descent_distribution.csv`
- Campo: `vict_descent`, `incidents`, `pct`
- Visual: Barras horizontales

KPI 8. Cantidad de delitos segun tipo de arma (Matias Kupfer)
- Tabla: `kpi_weapon_type_distribution.csv`
- Campo: `weapon_desc`, `incidents`
- Visual: Barras horizontales (Top N)

KPI 9. Cantidad de delitos segun sexo y edad (Victor Lozano)
- Tabla: `kpi_sex_age_distribution.csv`
- Campo: `vict_sex`, `age_bucket`, `incidents`
- Visual: Matriz o barras apiladas

Tip de modelado:
- Crea una tabla calendario (`Calendar`) para analisis temporal y relaciona con fecha de `fact_crimes_enriched.csv`.
- Si usas tablas agregadas para visuales de KPI, evita recalcular lo mismo desde la tabla detalle para no duplicar metricas.

## 6) Estructura sugerida de reporte Power BI

- Pagina 1: Resumen ejecutivo (tarjetas KPI + tendencia anual).
- Pagina 2: Analisis temporal (mensual/semanal/diario).
- Pagina 3: Analisis intradia (hora y 15 minutos).
- Pagina 4: Mapa y distribucion territorial (area, lat/lon).
- Pagina 5: Perfil de victimas y estado de casos.
- Pagina 6: Efecto de festivos (comparativo festivo vs no festivo).

## 7) Estructura sugerida del informe (max 20 paginas)

1. Introduccion.
2. Infraestructura BI y diagrama extremo a extremo.
3. Proceso ETL (fuentes, limpieza, reglas, enriquecimiento, data marts).
4. KPIs definidos y justificacion (al menos 3 por integrante).
5. Diseno del reporte Power BI (visualizaciones, colores, navegacion).
6. Conclusiones.
7. Anexos: codigo ETL y PBIX.
