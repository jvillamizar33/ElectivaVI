/**
 * mongodb_pipelines.js - UrbanPulse IoT
 * Autores: Juanjose Villamizar, Angello Corredor, Antony Marin - Docente: Camilo Fierro
 * Ejecutar con mongosh (después de correr etl_pipeline.py):
 *
 *     mongosh "mongodb://localhost:27017" mongodb_pipelines.js
 *
 * Contenido
 *   0. Decisión de modelado (incrustación vs. referencias)
 *   1. Validación de esquema ($jsonSchema) para la colección `lecturas`
 *   2. Índices
 *   3. Consulta 1: promedio por hora de contaminación (PM2.5) por zona  -> Aggregation Pipeline
 *   4. Consulta 2: alertas críticas de las últimas 24 horas              -> filtrado indexado
 *   5. Consultas complementarias y verificación con explain()
 */

// ---------------------------------------------------------------------------
// 0. DECISIÓN DE MODELADO
// ---------------------------------------------------------------------------
// - Lecturas: UN documento por lectura (colección `lecturas`).
//     * NO se incrustan las lecturas dentro del documento del sensor: son una serie de tiempo que
//       crece sin límite (anti-patrón "unbounded array"; límite de 16 MB por documento; cada $push
//       reescribe un documento cada vez más grande).
// - Datos de la zona: SE INCRUSTAN en cada lectura (zona.id, zona.nombre, ubicacion). Son pequeños,
//   casi inmutables y se consultan SIEMPRE junto con la lectura => evita $lookup (join) en las consultas
//   analíticas. Es desnormalización deliberada a cambio de lecturas rápidas.
// - Catálogo de sensores: colección aparte `sensores` (REFERENCIA por sensor_id) para metadatos
//   propios del sensor y su lista de zonas conectadas.
// - Campo `mediciones` flexible: cada tipo de sensor guarda solo sus métricas (aire vs. tráfico),
//   algo que un esquema relacional rígido obliga a resolver con columnas NULL o tablas extra.

const BD = db.getSiblingDB("urbanpulse");

// ---------------------------------------------------------------------------
// 1. VALIDACIÓN DE ESQUEMA (flexible: solo exige lo esencial)
// ---------------------------------------------------------------------------
BD.runCommand({
  collMod: "lecturas",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["sensor_id", "tipo_sensor", "zona", "timestamp", "mediciones", "nivel_alerta", "es_alerta_critica"],
      properties: {
        sensor_id: { bsonType: "string" },
        tipo_sensor: { enum: ["calidad_aire", "trafico"] },
        timestamp: { bsonType: "date" },
        nivel_alerta: { enum: ["normal", "moderado", "alto", "critico"] },
        es_alerta_critica: { bsonType: "bool" },
        "mediciones": { bsonType: "object" },
      },
    },
  },
  validationLevel: "moderate",
  validationAction: "warn",
});

// ---------------------------------------------------------------------------
// 2. ÍNDICES
// ---------------------------------------------------------------------------
// (a) Único: historial por sensor + evita duplicados en recargas del ETL.
BD.lecturas.createIndex({ sensor_id: 1, timestamp: 1 }, { unique: true, name: "uq_sensor_timestamp" });

// (b) Soporta el $match inicial del pipeline de contaminación: igualdad (tipo_sensor) + rango (timestamp).
//     Regla ESR (Equality, Sort, Range).
BD.lecturas.createIndex({ tipo_sensor: 1, timestamp: -1 }, { name: "idx_tipo_timestamp" });

// (c) Consultas por zona y ventana de tiempo.
BD.lecturas.createIndex({ "zona.id": 1, timestamp: -1 }, { name: "idx_zona_timestamp" });

// (d) ÍNDICE PARCIAL: solo indexa las alertas críticas (una fracción pequeña de la colección).
//     Ocupa mucho menos espacio y hace la consulta de "alertas críticas recientes" casi inmediata.
BD.lecturas.createIndex(
  { timestamp: -1 },
  { name: "idx_alertas_criticas_parcial", partialFilterExpression: { es_alerta_critica: true } }
);

// (e) Geoespacial (sensores/lecturas cercanas a un punto).
BD.lecturas.createIndex({ ubicacion: "2dsphere" }, { name: "idx_ubicacion_2dsphere" });
BD.sensores.createIndex({ ubicacion: "2dsphere" }, { name: "idx_sensores_ubicacion" });

print("Índices en `lecturas`:");
BD.lecturas.getIndexes().forEach((i) => print("  - " + i.name));

// ---------------------------------------------------------------------------
// Referencia temporal.
// Para datos en vivo cambie por:  const AHORA = new Date();
// Para el dataset de prueba (termina el 11/nov/2026 14:00 UTC) se fija la fecha de corte:
// ---------------------------------------------------------------------------
const AHORA = new Date("2026-11-11T14:00:00Z");
const HACE_24H = new Date(AHORA.getTime() - 24 * 60 * 60 * 1000);

// ---------------------------------------------------------------------------
// 3. CONSULTA 1 - PROMEDIO POR HORA DE CONTAMINACIÓN (PM2.5) POR ZONA
// ---------------------------------------------------------------------------
const pipelinePromedioHoraZona = [
  // $match PRIMERO: reduce documentos y usa idx_tipo_timestamp
  {
    $match: {
      tipo_sensor: "calidad_aire",
      timestamp: { $gte: HACE_24H, $lte: AHORA },
      "mediciones.pm25": { $exists: true },
    },
  },
  // Agrupa por zona + fecha + hora local (campos calculados en el ETL: no hace falta convertir zonas horarias aquí)
  {
    $group: {
      _id: { zona_id: "$zona.id", zona: "$zona.nombre", fecha: "$fecha", hora: "$hora" },
      pm25_promedio: { $avg: "$mediciones.pm25" },
      pm25_maximo: { $max: "$mediciones.pm25" },
      n_lecturas: { $sum: 1 },
    },
  },
  {
    $project: {
      _id: 0,
      zona_id: "$_id.zona_id",
      zona: "$_id.zona",
      fecha: "$_id.fecha",
      hora: "$_id.hora",
      pm25_promedio: { $round: ["$pm25_promedio", 2] },
      pm25_maximo: 1,
      n_lecturas: 1,
    },
  },
  { $sort: { zona_id: 1, fecha: 1, hora: 1 } },
];

print("\n=== Consulta 1: promedio horario de PM2.5 por zona (primeras 10 filas) ===");
BD.lecturas.aggregate(pipelinePromedioHoraZona).limit(10).forEach((d) => printjson(d));

// Variante: promedio por zona de TODA la ventana (ranking de zonas más contaminadas)
const pipelineRankingZonas = [
  { $match: { tipo_sensor: "calidad_aire" } },
  { $group: { _id: "$zona.nombre", pm25_promedio: { $avg: "$mediciones.pm25" }, n: { $sum: 1 } } },
  { $project: { _id: 0, zona: "$_id", pm25_promedio: { $round: ["$pm25_promedio", 2] }, n: 1 } },
  { $sort: { pm25_promedio: -1 } },
];
print("\n=== Ranking de zonas por PM2.5 promedio ===");
BD.lecturas.aggregate(pipelineRankingZonas).forEach((d) => printjson(d));

// ---------------------------------------------------------------------------
// 4. CONSULTA 2 - ALERTAS CRÍTICAS DE LAS ÚLTIMAS 24 HORAS (filtrado indexado)
// ---------------------------------------------------------------------------
// IMPORTANTE: el filtro incluye `es_alerta_critica: true`; sin él MongoDB no puede usar el índice parcial.
const filtroAlertas = { es_alerta_critica: true, timestamp: { $gte: HACE_24H, $lte: AHORA } };

print("\n=== Consulta 2: alertas críticas últimas 24 h (5 más recientes) ===");
BD.lecturas
  .find(filtroAlertas, { _id: 0, sensor_id: 1, tipo_sensor: 1, "zona.nombre": 1, timestamp: 1, mediciones: 1 })
  .sort({ timestamp: -1 })
  .limit(5)
  .forEach((d) => printjson(d));

print("Total de alertas críticas (24 h): " + BD.lecturas.countDocuments(filtroAlertas));

// Conteo de alertas críticas por zona y tipo (útil para el análisis de resultados)
print("\n=== Alertas críticas por zona y tipo de sensor (24 h) ===");
BD.lecturas
  .aggregate([
    { $match: filtroAlertas },
    { $group: { _id: { zona: "$zona.nombre", tipo: "$tipo_sensor" }, alertas: { $sum: 1 } } },
    { $sort: { alertas: -1 } },
  ])
  .forEach((d) => printjson(d));

// ---------------------------------------------------------------------------
// 5. VERIFICACIÓN DE RENDIMIENTO CON explain()
// ---------------------------------------------------------------------------
// Qué buscar:  winningPlan -> IXSCAN (no COLLSCAN)  y  totalDocsExamined ≈ nReturned.
function resumenExplain(nombre, e) {
  const s = e.executionStats;
  const plan = JSON.stringify(e.queryPlanner.winningPlan);
  const indice = (plan.match(/"indexName":"([^"]+)"/) || [])[1];
  print(`\n[explain] ${nombre}\n  etapa: ${plan.includes("IXSCAN") ? "IXSCAN" : "COLLSCAN"} | índice: ${indice}` +
        `\n  nReturned=${s.nReturned}  totalKeysExamined=${s.totalKeysExamined}  totalDocsExamined=${s.totalDocsExamined}` +
        `  ms=${s.executionTimeMillis}`);
}

resumenExplain("Alertas críticas 24 h", BD.lecturas.find(filtroAlertas).sort({ timestamp: -1 }).explain("executionStats"));
resumenExplain("Pipeline PM2.5 (stage $match)", BD.lecturas.explain("executionStats").aggregate(pipelinePromedioHoraZona));

// Comparación: la misma consulta de alertas SIN índices (forzando recorrido de colección)
const sinIndice = BD.lecturas.find(filtroAlertas).hint({ $natural: 1 }).explain("executionStats").executionStats;
print(`\n[comparación] Alertas con COLLSCAN -> totalDocsExamined=${sinIndice.totalDocsExamined}, ms=${sinIndice.executionTimeMillis}`);

// ---------------------------------------------------------------------------
// 6. EXTRA - sensores a menos de 1 km del Hospital Regional (usa el índice 2dsphere)
// ---------------------------------------------------------------------------
print("\n=== Sensores a <= 1 km del Hospital Regional ===");
BD.sensores
  .find({
    ubicacion: {
      $near: { $geometry: { type: "Point", coordinates: [-73.8622, 7.0553] }, $maxDistance: 1000 },
    },
  })
  .limit(5)
  .forEach((s) => printjson({ sensor: s._id, tipo: s.tipo_sensor, zona: s.zona.nombre }));
