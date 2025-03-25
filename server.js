// Database configuration
const dbConfig = {
  host: process.env.DB_HOST || "127.0.0.1",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "Atenas9democraci.",
  database: process.env.DB_NAME || "thera_final_database",
  port: process.env.DB_PORT || 3306,
  connectTimeout: 60000,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
};

// Create the connection pool instead of a single connection
const pool = mysql.createPool(dbConfig);

// Add a connection test
pool.getConnection((err, connection) => {
  if (err) {
    console.error("Error connecting to the database:", err);
    return;
  }
  console.log("Successfully connected to database");
  connection.release();
});
