const fs = require("fs");
const path = require("path");
const db = require("./db");

const DATA_ROOT = path.resolve(__dirname, "../data");
let initPromise = null;

function quoteIdentifier(identifier) {
  return `"${String(identifier).replace(/"/g, '""')}"`;
}

function normalizeValue(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  return String(value);
}

function promisifyRun(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function runCallback(error) {
      if (error) {
        reject(error);
        return;
      }

      resolve(this);
    });
  });
}

function promisifyPrepareRun(stmt, params = []) {
  return new Promise((resolve, reject) => {
    stmt.run(params, function runCallback(error) {
      if (error) {
        reject(error);
        return;
      }

      resolve(this);
    });
  });
}

function promisifyFinalize(stmt) {
  return new Promise((resolve, reject) => {
    stmt.finalize((error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

async function loadJsonLinesFile(filePath) {
  const content = await fs.promises.readFile(filePath, "utf8");

  return content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function loadDirectoryRows(directoryName) {
  const directoryPath = path.join(DATA_ROOT, directoryName);
  const entries = await fs.promises.readdir(directoryPath, {
    withFileTypes: true,
  });
  const fileNames = entries
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .filter((name) => name.endsWith(".jsonl") || name.endsWith(".json"))
    .sort();

  const rows = [];

  for (const fileName of fileNames) {
    const fileRows = await loadJsonLinesFile(
      path.join(directoryPath, fileName),
    );
    rows.push(...fileRows);
  }

  return rows;
}

async function loadTables() {
  const entries = await fs.promises.readdir(DATA_ROOT, { withFileTypes: true });
  const tables = [];

  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }

    const rows = await loadDirectoryRows(entry.name);

    if (rows.length === 0) {
      continue;
    }

    const columns = new Set();

    rows.forEach((row) => {
      Object.keys(row).forEach((key) => columns.add(key));
    });

    tables.push({
      tableName: entry.name,
      rows,
      columns: Array.from(columns),
    });
  }

  return tables;
}

function buildSchemaSummary(tables) {
  return tables
    .map(({ tableName, columns }) => {
      const previewColumns = columns.slice(0, 30).join(", ");
      const suffix = columns.length > 30 ? ", ..." : "";
      return `${tableName}(${previewColumns}${suffix})`;
    })
    .join("\n");
}

async function initDB() {
  if (!initPromise) {
    initPromise = (async () => {
      const tables = await loadTables();

      await promisifyRun("PRAGMA foreign_keys = OFF");
      await promisifyRun("PRAGMA journal_mode = WAL");
      await promisifyRun("PRAGMA synchronous = NORMAL");

      for (const table of tables) {
        const quotedTableName = quoteIdentifier(table.tableName);
        const columnDefinitions = table.columns
          .map((column) => `${quoteIdentifier(column)} TEXT`)
          .join(", ");

        await promisifyRun(`DROP TABLE IF EXISTS ${quotedTableName}`);
        await promisifyRun(
          `CREATE TABLE ${quotedTableName} (${columnDefinitions})`,
        );
        await promisifyRun("BEGIN TRANSACTION");

        const columnsSql = table.columns.map(quoteIdentifier).join(", ");
        const placeholders = table.columns.map(() => "?").join(", ");
        const insertSql = `INSERT INTO ${quotedTableName} (${columnsSql}) VALUES (${placeholders})`;
        const stmt = db.prepare(insertSql);

        for (const row of table.rows) {
          const values = table.columns.map((column) =>
            normalizeValue(row[column]),
          );
          await promisifyPrepareRun(stmt, values);
        }

        await promisifyFinalize(stmt);
        await promisifyRun("COMMIT");
      }

      return {
        db,
        tables,
        schemaSummary: buildSchemaSummary(tables),
      };
    })();
  }

  return initPromise;
}

if (require.main === module) {
  initDB()
    .then(() => {
      console.log("DB ready");
    })
    .catch((error) => {
      console.error(error);
      process.exit(1);
    });
}

module.exports = initDB;
