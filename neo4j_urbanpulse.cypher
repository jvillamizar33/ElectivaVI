// =============================================================================
// neo4j_urbanpulse.cypher - UrbanPulse IoT (Neo4j 5.x)
// Autores: Juanjose Villamizar, Angello Corredor, Antony Marin - Docente: Camilo Fierro
// =============================================================================
// PRE-REQUISITO: copiar los CSV generados por etl_pipeline.py (datasets/procesado/*.csv)
// a la carpeta `import` de la instancia de Neo4j:
//     zonas.csv, sensores.csv, conexiones.csv, pasos_vehiculos.csv
// (Neo4j Desktop: instancia > ... > Open folder > Import).
// Ejecutar en Neo4j Browser o:  cypher-shell -f neo4j_urbanpulse.cypher
//
// MODELO DEL GRAFO
//   (:Zona      {id, nombre, latitud, longitud, pm25_promedio, velocidad_promedio, trafico_critico})
//   (:Sensor    {id, tipo, latitud, longitud})
//   (:Vehiculo  {placa})
//   (:Sensor)-[:UBICADO_EN]->(:Zona)
//   (:Vehiculo)-[:REGISTRO_PASO_POR {timestamp, velocidad_kmh}]->(:Sensor)   // el sensor registró el paso del vehículo
//   (:Zona)-[:CONECTADO_A {distancia_km}]-(:Zona)                            // vías entre zonas (se consulta sin dirección)
// =============================================================================


// -----------------------------------------------------------------------------
// 0. (Opcional) Reiniciar la base antes de volver a cargar
// -----------------------------------------------------------------------------
// MATCH (n) DETACH DELETE n;


// -----------------------------------------------------------------------------
// 1. RESTRICCIONES E ÍNDICES  (la unicidad crea el índice y habilita MERGE rápido)
// -----------------------------------------------------------------------------
CREATE CONSTRAINT zona_id IF NOT EXISTS FOR (z:Zona) REQUIRE z.id IS UNIQUE;
CREATE CONSTRAINT sensor_id IF NOT EXISTS FOR (s:Sensor) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT vehiculo_placa IF NOT EXISTS FOR (v:Vehiculo) REQUIRE v.placa IS UNIQUE;
CREATE INDEX zona_trafico_critico IF NOT EXISTS FOR (z:Zona) ON (z.trafico_critico);


// -----------------------------------------------------------------------------
// 2. CREACIÓN E INGESTA DEL GRAFO  (consulta 1 del enunciado)
// -----------------------------------------------------------------------------
// 2.1 Nodos :Zona
LOAD CSV WITH HEADERS FROM 'file:///zonas.csv' AS row
MERGE (z:Zona {id: row.id})
SET z.nombre = row.nombre,
    z.latitud = toFloat(row.latitud),
    z.longitud = toFloat(row.longitud),
    z.pm25_promedio = toFloat(row.pm25_promedio),
    z.velocidad_promedio = toFloat(row.velocidad_promedio),
    z.trafico_critico = (row.trafico_critico = 'true');

// 2.2 Nodos :Sensor + relación [:UBICADO_EN]
LOAD CSV WITH HEADERS FROM 'file:///sensores.csv' AS row
MERGE (s:Sensor {id: row.id})
SET s.tipo = row.tipo,
    s.latitud = toFloat(row.latitud),
    s.longitud = toFloat(row.longitud)
WITH s, row
MATCH (z:Zona {id: row.zona_id})
MERGE (s)-[:UBICADO_EN]->(z);

// 2.3 Relaciones [:CONECTADO_A] entre zonas (se guarda en un sentido; las consultas la recorren sin dirección)
LOAD CSV WITH HEADERS FROM 'file:///conexiones.csv' AS row
MATCH (a:Zona {id: row.origen}), (b:Zona {id: row.destino})
MERGE (a)-[c:CONECTADO_A]->(b)
SET c.distancia_km = toFloat(row.distancia_km);

// 2.4 Nodos :Vehiculo + relación [:REGISTRO_PASO_POR]
//     (484 filas en el dataset de prueba; con volúmenes grandes use:  CALL { ... } IN TRANSACTIONS OF 1000 ROWS)
LOAD CSV WITH HEADERS FROM 'file:///pasos_vehiculos.csv' AS row
MERGE (v:Vehiculo {placa: row.placa})
WITH v, row
MATCH (s:Sensor {id: row.sensor_id})
MERGE (v)-[r:REGISTRO_PASO_POR {timestamp: datetime(row.timestamp)}]->(s)
SET r.velocidad_kmh = toFloat(row.velocidad_kmh);

// 2.5 Verificación de la carga
MATCH (n) RETURN labels(n)[0] AS etiqueta, count(*) AS nodos ORDER BY nodos DESC;
MATCH ()-[r]->() RETURN type(r) AS relacion, count(*) AS total ORDER BY total DESC;


// -----------------------------------------------------------------------------
// 3. CAMINO MÁS CORTO ENTRE ZONAS CON TRÁFICO CRÍTICO  (consulta 2 del enunciado)
// -----------------------------------------------------------------------------
// 3.1 Entre dos zonas concretas (Parque Industrial -> Hospital Regional), en saltos
MATCH (a:Zona {id: 'Z03'}), (b:Zona {id: 'Z06'})
MATCH p = shortestPath((a)-[:CONECTADO_A*..10]-(b))
RETURN [n IN nodes(p) | n.nombre] AS ruta,
       length(p) AS saltos,
       round(reduce(d = 0.0, r IN relationships(p) | d + r.distancia_km), 3) AS distancia_km;

// 3.2 Para TODOS los pares de zonas con tráfico crítico (a.id < b.id evita duplicar pares y comparar un nodo consigo mismo)
MATCH (a:Zona {trafico_critico: true}), (b:Zona {trafico_critico: true})
WHERE a.id < b.id
MATCH p = shortestPath((a)-[:CONECTADO_A*..10]-(b))
RETURN a.nombre AS origen, b.nombre AS destino,
       [n IN nodes(p) | n.nombre] AS ruta,
       length(p) AS saltos,
       round(reduce(d = 0.0, r IN relationships(p) | d + r.distancia_km), 3) AS distancia_km
ORDER BY saltos, distancia_km;

// 3.3 Ruta más corta en KILÓMETROS (ponderada). shortestPath() minimiza saltos, no distancia.
//     En un grafo pequeño basta con enumerar caminos; en grafos grandes use APOC (apoc.algo.dijkstra) o GDS.
MATCH (a:Zona {id: 'Z03'}), (b:Zona {id: 'Z06'})
MATCH p = (a)-[:CONECTADO_A*..6]-(b)
WITH p, reduce(d = 0.0, r IN relationships(p) | d + r.distancia_km) AS km
RETURN [n IN nodes(p) | n.nombre] AS ruta, length(p) AS saltos, round(km, 3) AS distancia_km
ORDER BY km ASC
LIMIT 1;


// -----------------------------------------------------------------------------
// 4. NODOS MÁS RELEVANTES  (consulta 3 del enunciado)
// -----------------------------------------------------------------------------
// 4.1 Centralidad de grado: número de zonas vecinas (cuántas vías convergen en la zona)
MATCH (z:Zona)
RETURN z.nombre AS zona,
       COUNT { (z)-[:CONECTADO_A]-() } AS grado,
       z.trafico_critico AS trafico_critico
ORDER BY grado DESC, zona;

// 4.2 Conexiones significativas: flujo vehicular por zona (pasos registrados y vehículos distintos)
MATCH (v:Vehiculo)-[r:REGISTRO_PASO_POR]->(s:Sensor)-[:UBICADO_EN]->(z:Zona)
RETURN z.nombre AS zona,
       count(r) AS pasos_registrados,
       count(DISTINCT v) AS vehiculos_distintos
ORDER BY pasos_registrados DESC;

// 4.3 Índice combinado: estructura de la red (grado) + flujo real (pasos)
MATCH (z:Zona)
OPTIONAL MATCH (z)<-[:UBICADO_EN]-(:Sensor)<-[r:REGISTRO_PASO_POR]-(:Vehiculo)
WITH z, count(r) AS pasos, COUNT { (z)-[:CONECTADO_A]-() } AS grado
RETURN z.nombre AS zona, grado, pasos, grado * pasos AS indice_relevancia
ORDER BY indice_relevancia DESC;

// 4.4 Algoritmos de centralidad con Graph Data Science (requiere el plugin GDS; si no lo tiene, use 4.1-4.3)
// CALL gds.graph.project('urbanpulse', 'Zona', {CONECTADO_A: {orientation: 'UNDIRECTED'}});
// CALL gds.betweenness.stream('urbanpulse') YIELD nodeId, score
//   RETURN gds.util.asNode(nodeId).nombre AS zona, round(score, 2) AS intermediacion ORDER BY intermediacion DESC;
// CALL gds.pageRank.stream('urbanpulse') YIELD nodeId, score
//   RETURN gds.util.asNode(nodeId).nombre AS zona, round(score, 4) AS pagerank ORDER BY pagerank DESC;
// CALL gds.graph.drop('urbanpulse');


// -----------------------------------------------------------------------------
// 5. CONSULTAS AVANZADAS ADICIONALES (patrones)
// -----------------------------------------------------------------------------
// 5.1 Vehículos que pasaron por 2 o más zonas con tráfico crítico
MATCH (v:Vehiculo)-[:REGISTRO_PASO_POR]->(:Sensor)-[:UBICADO_EN]->(z:Zona {trafico_critico: true})
WITH v, collect(DISTINCT z.nombre) AS zonas
WHERE size(zonas) >= 2
RETURN v.placa AS vehiculo, zonas, size(zonas) AS zonas_criticas_visitadas
ORDER BY zonas_criticas_visitadas DESC, vehiculo
LIMIT 10;

// 5.2 Vecinos a 2 saltos de una zona crítica (zonas que podrían recibir el desvío del tráfico)
MATCH (z:Zona {id: 'Z06'})-[:CONECTADO_A*1..2]-(vecina:Zona)
WHERE vecina <> z
RETURN DISTINCT vecina.nombre AS zona_alterna, vecina.trafico_critico AS tambien_critica
ORDER BY zona_alterna;
