require("dotenv").config();
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const path = require("path");
const { spawn } = require("child_process");
const mysql = require("mysql2");
const { Parser } = require("json2csv");

const app = express();

// Añadir al principio del archivo, después de los requires
const log = (message) => {
  console.log(`[${new Date().toISOString()}] ${message}`);
};

// Database configuration
const dbConfig = {
  host: process.env.DB_HOST || "127.0.0.1", // Connect to local proxy
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "Atenas9democraci.",
  database: process.env.DB_NAME || "thera_final_database",
  port: process.env.DB_PORT || 3306,
  connectTimeout: 60000,
};

// Create the connection pool
const pool = mysql.createPool(dbConfig);

app.use(
  cors({
    origin: "http://localhost:3000", // Your frontend URL
    credentials: true,
  })
);
app.use(express.json());

// Configure multer for file upload
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    // Create absolute path to uploads directory
    const uploadsDir = path.join(__dirname, "uploads");
    cb(null, uploadsDir);
  },
  filename: function (req, file, cb) {
    cb(null, Date.now() + "-" + file.originalname);
  },
});

const upload = multer({ storage: storage });

// File upload endpoint
app.post("/api/upload", upload.single("file"), (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ message: "No file uploaded" });
    }

    console.log("File received:", {
      filename: req.file.filename,
      path: req.file.path,
      mimetype: req.file.mimetype,
      size: req.file.size,
    });

    console.log("Source type:", req.body.source);

    const sourceType = req.body.source.replace(/ /g, "_"); // Replace spaces with underscores
    const pythonScriptPath = path.join(__dirname, "data_processor.py");

    console.log("Running Python script:", {
      script: pythonScriptPath,
      file: req.file.path,
      source: sourceType,
    });

    const pythonProcess = spawn("python", [
      pythonScriptPath,
      "--file",
      req.file.path,
      "--source",
      sourceType,
    ]);

    let result = "";
    let error = "";

    pythonProcess.stdout.on("data", (data) => {
      result += data.toString();
      console.log("Python stdout:", data.toString());
    });

    pythonProcess.stderr.on("data", (data) => {
      error += data.toString();
      console.log("Python stderr:", data.toString());
    });

    pythonProcess.on("close", (code) => {
      console.log("Python process exited with code:", code);
      if (code !== 0) {
        return res
          .status(500)
          .json({ message: "Error processing file", error });
      }
      res.json({ message: "File processed successfully", result });
    });
  } catch (error) {
    console.error("Server error:", error);
    res
      .status(500)
      .json({ message: "Error processing file", error: error.message });
  }
});

// Reconciliation endpoint
app.post("/api/reconcile", async (req, res) => {
  try {
    console.log("Starting reconciliation process...");
    const pythonProcess = spawn("python", [
      "reconciliation_service.py",
      "--reconcile",
    ]);

    let output = "";
    let errorOutput = "";

    pythonProcess.stdout.on("data", (data) => {
      const chunk = data.toString();
      console.log("Python output:", chunk);
      output += chunk;
    });

    pythonProcess.stderr.on("data", (data) => {
      const chunk = data.toString();
      console.error("Python error:", chunk);
      errorOutput += chunk;
    });

    pythonProcess.on("close", (code) => {
      console.log("Python process exited with code:", code);

      if (code !== 0) {
        return res.status(500).json({
          error: "Reconciliation failed",
          details: errorOutput,
        });
      }

      try {
        // Look for the last line that contains JSON
        const lines = output.split("\n");
        const jsonLine = lines.find(
          (line) => line.trim().startsWith("{") && line.trim().endsWith("}")
        );

        if (!jsonLine) {
          throw new Error("No JSON found in output");
        }

        const result = JSON.parse(jsonLine);
        res.json(result);
      } catch (e) {
        console.error("Error parsing Python output:", e);
        res.status(500).json({
          error: "Failed to parse reconciliation result",
          details: e.message,
          output: output,
        });
      }
    });

    pythonProcess.on("error", (error) => {
      console.error("Failed to start Python process:", error);
      res.status(500).json({
        error: "Failed to start reconciliation process",
        details: error.message,
      });
    });
  } catch (error) {
    console.error("Server error:", error);
    res.status(500).json({
      error: "Server error during reconciliation",
      details: error.message,
    });
  }
});

// Add this after your other endpoints
app.get("/api/sources/:sourceId", async (req, res) => {
  try {
    const sourceId = req.params.sourceId;
    const pythonScriptPath = path.join(__dirname, "data_processor.py");

    const pythonProcess = spawn("python", [
      pythonScriptPath,
      "--get-source",
      "--source-id",
      sourceId,
    ]);

    let result = "";
    let error = "";

    pythonProcess.stdout.on("data", (data) => {
      result += data.toString();
    });

    pythonProcess.stderr.on("data", (data) => {
      error += data.toString();
      console.log("Python stderr:", data.toString());
    });

    pythonProcess.on("close", (code) => {
      if (code !== 0) {
        return res.status(500).json({
          message: "Error fetching source data",
          error,
        });
      }

      try {
        const jsonResult = JSON.parse(result);
        res.json(jsonResult);
      } catch (parseError) {
        console.error("Error parsing Python output:", result);
        res.status(500).json({
          message: "Error parsing source data",
          error: parseError.message,
        });
      }
    });
  } catch (error) {
    console.error("Server error:", error);
    res.status(500).json({
      message: "Error fetching source data",
      error: error.message,
    });
  }
});

app.post("/api/unmatched-transactions", async (req, res) => {
  const { startDate, endDate, transactionType } = req.body;

  try {
    const conn = await mysql.createConnection(dbConfig);

    // Determine which table to query based on transaction type
    const tableName =
      transactionType === "started" ? "started_matches" : "succeeded_matches";

    // Get unmatched transactions (where _merge = 'right_only')
    const query = `
            SELECT 
                id,
                amount,
                currency,
                \`Created date (UTC)\` as created_date,
                status,
                PaymentIntent_ID
            FROM ${tableName}
            WHERE _merge = 'right_only'
            AND \`Created date (UTC)\` BETWEEN ? AND ?
            ORDER BY \`Created date (UTC)\`
        `;

    const [rows] = await conn.execute(query, [startDate, endDate]);

    // Convert to CSV
    const csvFields = [
      "ID",
      "Amount",
      "Currency",
      "Created Date",
      "Status",
      "PaymentIntent ID",
    ];
    const json2csvParser = new Parser({ fields: csvFields });
    const csv = json2csvParser.parse(rows);

    res.setHeader("Content-Type", "text/csv");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename=unmatched_${transactionType}_transactions.csv`
    );
    res.send(csv);
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Error generating report");
  }
});

// Modify this endpoint
app.post("/api/transactions", async (req, res) => {
  try {
    const conn = await mysql.createConnection(dbConfig).promise();

    // 1. Verificar si la tabla existe
    const [tables] = await conn.query("SHOW TABLES LIKE 'started_matches'");
    console.log("Table exists:", tables.length > 0);

    if (tables.length === 0) {
      await conn.end();
      return res.status(404).json({
        message: "No data available. Please run reconciliation first.",
      });
    }

    // 2. Ver la estructura de la tabla
    const [columns] = await conn.query("DESCRIBE started_matches");
    console.log("Table structure:", columns);

    // 3. Contar todos los registros
    const [countRows] = await conn.query(
      "SELECT COUNT(*) as total FROM started_matches"
    );
    console.log("Total records:", countRows[0].total);

    // 4. Contar registros right_only
    const [rightOnlyRows] = await conn.query(
      "SELECT COUNT(*) as total FROM started_matches WHERE _merge = 'right_only'"
    );
    console.log("Right only records:", rightOnlyRows[0].total);

    // 5. Ver una muestra de los datos
    const [sampleRows] = await conn.query(
      "SELECT * FROM started_matches WHERE _merge = 'right_only' LIMIT 1"
    );
    console.log("Sample right_only row:", sampleRows[0]);

    // 6. Obtener las transacciones
    const [rows] = await conn.query(`
      SELECT 
        id,
        effective_date,
        metadata_type,
        metadata_latestStripeChargeId,
        metadata_paymentId,
        amount,
        currency,
        created_date,
        status,
        PaymentIntent_ID,
        _merge
      FROM started_matches 
      WHERE _merge = 'right_only'
      ORDER BY effective_date DESC 
      LIMIT 50
    `);
    console.log("Found transactions:", rows.length);

    await conn.end();
    res.json(rows);
  } catch (error) {
    console.error("Error:", error);
    res.status(500).json({
      message: "Error fetching transactions",
      error: error.message,
    });
  }
});

// Add export endpoint
app.post("/api/export-transactions", async (req, res) => {
  // Similar to /api/transactions but returns CSV
  // ... implementation similar to your existing export endpoint
});

// Add this new endpoint
app.get("/api/summary", async (req, res) => {
  try {
    const conn = await mysql.createConnection(dbConfig);

    // Get total transactions
    const [totalRows] = await conn.execute(
      "SELECT COUNT(*) as count FROM Thera_Ledger_Transactions"
    );
    const totalTransactions = totalRows[0].count;

    // Get reconciled amount
    const [reconciledRows] = await conn.execute(`
      SELECT SUM(amount) as total 
      FROM Thera_Ledger_Transactions 
      WHERE metadata_stripeBalanceTrxId IS NOT NULL
    `);
    const reconciled = reconciledRows[0].total;

    // Get exceptions count
    const [exceptionRows] = await conn.execute(`
      SELECT COUNT(*) as count 
      FROM started_matches 
      WHERE _merge = 'right_only'
    `);
    const exceptions = exceptionRows[0].count;

    // Get pending uploads
    const [pendingRows] = await conn.execute(`
      SELECT COUNT(*) as count 
      FROM Thera_Ledger_Transactions 
      WHERE metadata_stripeBalanceTrxId IS NULL
    `);
    const pending = pendingRows[0].count;

    res.json({
      totalTransactions: totalTransactions || 0,
      reconciled: reconciled || 0,
      exceptions: exceptions || 0,
      pendingUploads: pending || 0,
    });

    await conn.end();
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("Error fetching summary data");
  }
});

app.get("/api/matches", async (req, res) => {
  try {
    log("Received request for matches");
    log(`Query params: ${JSON.stringify(req.query)}`);

    const matchType = req.query.type || "started";
    const filters = {
      date_from: req.query.date_from,
      date_to: req.query.date_to,
    };

    // Get absolute path to Python script
    const scriptPath = path.join(__dirname, "reconciliation_service.py");
    log(`Python script path: ${scriptPath}`);

    // Verify script exists
    if (!require("fs").existsSync(scriptPath)) {
      log(`Error: Python script not found at ${scriptPath}`);
      return res.status(500).json({
        error: "Server configuration error",
        details: "Python script not found",
      });
    }

    const pythonArgs = [
      scriptPath,
      "--get-matches",
      "--match-type",
      matchType,
      "--filters",
      JSON.stringify(filters),
    ];

    log(`Spawning Python process with args: ${JSON.stringify(pythonArgs)}`);

    const pythonProcess = spawn("python", pythonArgs);

    let output = "";
    let errorOutput = "";

    pythonProcess.stdout.on("data", (data) => {
      const chunk = data.toString();
      log(`Python stdout: ${chunk}`);
      output += chunk;
    });

    pythonProcess.stderr.on("data", (data) => {
      const chunk = data.toString();
      log(`Python stderr: ${chunk}`);
      errorOutput += chunk;
    });

    pythonProcess.on("error", (error) => {
      log(`Failed to start Python process: ${error.message}`);
      return res.status(500).json({
        error: "Failed to start Python process",
        details: error.message,
      });
    });

    pythonProcess.on("close", (code) => {
      log(`Python process exited with code: ${code}`);
      log(`Full output: ${output}`);
      if (errorOutput) log(`Full error output: ${errorOutput}`);

      if (code !== 0) {
        return res.status(500).json({
          error: "Failed to get matches",
          details: errorOutput || "Unknown error",
          code: code,
        });
      }

      try {
        log("Attempting to parse output...");
        const result = JSON.parse(output);
        log(`Parsed result: ${JSON.stringify(result)}`);

        if (!result.matches || !Array.isArray(result.matches)) {
          throw new Error("Invalid matches data structure");
        }

        if (result.matches.length === 0) {
          return res.status(404).json({ error: "No matches found" });
        }

        // Set headers for CSV download
        res.setHeader("Content-Type", "text/csv");
        res.setHeader(
          "Content-Disposition",
          `attachment; filename=${matchType}_matches_${
            new Date().toISOString().split("T")[0]
          }.csv`
        );

        // Convert to CSV
        const parser = new Parser({
          fields: Object.keys(result.matches[0]),
          delimiter: ",",
          quote: '"',
        });

        const csv = parser.parse(result.matches);
        res.send(csv);
      } catch (e) {
        log(`Error processing matches: ${e.message}`);
        log(`Error stack: ${e.stack}`);
        res.status(500).json({
          error: "Invalid response from Python script",
          details: e.message,
          output: output,
          stack: e.stack,
        });
      }
    });
  } catch (error) {
    log(`Error in /api/matches: ${error.message}`);
    res.status(500).json({
      error: error.message,
      stack: error.stack,
    });
  }
});

// Add this route to handle transaction list requests
app.get("/api/transactions", (req, res) => {
  const table = req.query.table || "started_matches";
  const python = spawn("/Users/teitozg/anaconda3/bin/python3", [
    "transaction_service.py",
    table,
  ]);
  let dataString = "";

  python.stdout.on("data", (data) => {
    dataString += data.toString();
  });

  python.stderr.on("data", (data) => {
    console.error("Python stderr:", data.toString());
  });

  python.on("close", (code) => {
    if (code !== 0) {
      return res.status(500).json({ error: "Failed to fetch transactions" });
    }
    try {
      const transactions = JSON.parse(dataString);
      res.json(transactions);
    } catch (e) {
      res.status(500).json({ error: "Invalid JSON response" });
    }
  });
});

const PORT = process.env.PORT || 5001;

app.get("/", (req, res) => {
  res.send("Thera Backend API is running");
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
