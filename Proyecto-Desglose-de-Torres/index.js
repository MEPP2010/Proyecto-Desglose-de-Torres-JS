import express from "express";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import path from "path";
import { fileURLToPath } from "url";

// Configuración de rutas absolutas
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

// Configuración de middlewares
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// Conexión a SQLite
const dbPromise = open({
  filename: path.join(__dirname, "desglose_torres.db"),
  driver: sqlite3.Database,
});

// --- Rutas HTML ---
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/calculadora", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "calculadora.html"));
});

// --- API: Opciones de Filtros ---
app.get("/api/options", async (req, res) => {
  const db = await dbPromise;
  const filters = {
    TIPO: req.query.TIPO?.trim() || "",
    FABRICANTE: req.query.FABRICANTE?.trim() || "",
    CABEZA: req.query.CABEZA?.trim() || "",
    CUERPO: req.query.CUERPO?.trim() || "",
    TRAMO: req.query.TRAMO?.trim() || "",
  };

  const fieldsToQuery = ["TIPO", "FABRICANTE", "CABEZA", "CUERPO", "PARTE_DIVISION", "TRAMO"];
  const options = {};

  try {
    for (const field of fieldsToQuery) {
      let query = `SELECT DISTINCT TRIM(${field}) AS value FROM piezas WHERE ${field} IS NOT NULL AND TRIM(${field}) != ''`;
      const params = [];

      for (const [key, value] of Object.entries(filters)) {
        if (value && key !== field) {
          query += ` AND ${key} = ?`;
          params.push(value);
        }
      }

      query += ` ORDER BY ${field}`;
      const rows = await db.all(query, params);
      options[field] = rows.map((r) => r.value);
    }

    res.json({ success: true, options });
  } catch (err) {
    console.error("Error en /api/options:", err);
    res.status(500).json({ success: false, message: "Error interno del servidor al obtener opciones." });
  }
});

// --- API: Búsqueda ---
app.get("/api/search", async (req, res) => {
  const db = await dbPromise;
  const filters = {
    tipo: req.query.tipo?.trim() || "",
    fabricante: req.query.fabricante?.trim() || "",
    cabeza: req.query.cabeza?.trim() || "",
    parte: req.query.parte?.trim() || "",
    cuerpo: req.query.cuerpo?.trim() || "",
    tramo: req.query.tramo?.trim() || "",
  };

  let query = "SELECT * FROM piezas WHERE 1=1";
  const params = [];

  for (const [key, value] of Object.entries(filters)) {
    if (value) {
      query += ` AND UPPER(${key.toUpperCase()}) = UPPER(?)`;
      params.push(value);
    }
  }

  query += " LIMIT 500";

  try {
    const rows = await db.all(query, params);
    res.json({ success: true, count: rows.length, results: rows });
  } catch (err) {
    console.error("Error en /api/search:", err);
    res.status(500).json({ success: false, message: "Error interno al buscar datos." });
  }
});

// --- API: Calcular materiales ---
const PARTS_DIV_2 = new Set(["BGDA", "BSUP", "BMED", "BINF", "BDER", "BIZQ", "BSUP/MED"]);
const PARTS_DIV_4 = new Set(["PATA 0", "PATA 0.0", "PATA 1.5", "PATA 3", "PATA 3.0", "PATA 4.5", "PATA 6", "PATA 6.0", "PATA 7.5", "PATA 9", "PATA 9.0"]);

app.post("/api/calculate", async (req, res) => {
  const db = await dbPromise;
  try {
    const { filters = {}, parts = [] } = req.body;

    if (!parts.length) {
      return res.status(400).json({ success: false, message: "Debe seleccionar al menos una parte." });
    }

    let query = "SELECT * FROM piezas WHERE 1=1";
    const params = [];

    for (const field of ["tipo", "fabricante", "cabeza"]) {
      const val = filters[field]?.trim();
      if (val) {
        query += ` AND ${field.toUpperCase()} = ?`;
        params.push(val);
      }
    }

    const allPieces = await db.all(query, params);
    const calculated = [];

    for (const piece of allPieces) {
      const parteDivision = (piece.PARTE_DIVISION || "").trim().toUpperCase();
      if (!parteDivision) continue;

      const cantidadOriginal = parseFloat(piece.CANTIDAD_X_TORRE || 0);
      let cantidadCalculada = 0;

      for (const selected of parts) {
        const partName = selected.part.trim().toUpperCase();
        const qty = selected.quantity || 0;

        if (parteDivision === partName) {
          if (PARTS_DIV_2.has(parteDivision)) cantidadCalculada += (cantidadOriginal * qty) / 2;
          else if (PARTS_DIV_4.has(parteDivision)) cantidadCalculada += (cantidadOriginal * qty) / 4;
          else cantidadCalculada += cantidadOriginal * qty;
        }
      }

      if (cantidadCalculada > 0) {
        const pesoUnitario = parseFloat(piece.PESO_UNITARIO || 0);
        const pesoTotal = cantidadCalculada * pesoUnitario;
        calculated.push({
          ...piece,
          CANTIDAD_ORIGINAL: cantidadOriginal,
          CANTIDAD_CALCULADA: cantidadCalculada,
          PESO_TOTAL: pesoTotal,
        });
      }
    }

    const totalPiezas = calculated.reduce((sum, p) => sum + p.CANTIDAD_CALCULADA, 0);
    const totalPeso = calculated.reduce((sum, p) => sum + p.PESO_TOTAL, 0);

    res.json({
      success: true,
      count: calculated.length,
      results: calculated,
      totals: { total_pieces: totalPiezas, total_weight: totalPeso },
    });
  } catch (err) {
    console.error("Error en /api/calculate:", err);
    res.status(500).json({ success: false, message: "Error al calcular materiales." });
  }
});

// --- Servidor ---
app.listen(PORT, () => {
  console.log(`🚀 Servidor escuchando en http://localhost:${PORT}`);
});

module.exports = app;
