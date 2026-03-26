const fs = require("fs");
const path = require("path");
const sqlite3 = require("sqlite3").verbose();
const askLLM = require("./llm");
const askSQL = askLLM.askSQL;
const initDB = require("./initDB");

const DATA_ROOT = path.resolve(__dirname, "../data");
const MAX_RESULT_ROWS = 25;

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

function parseJsonResponse(text) {
  const trimmed = text.trim();

  try {
    return JSON.parse(trimmed);
  } catch (error) {
    const match = trimmed.match(/\{[\s\S]*\}/);

    if (!match) {
      throw error;
    }

    return JSON.parse(match[0]);
  }
}

function promisifyRun(db, sql, params = []) {
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

function promisifyAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => {
      if (error) {
        reject(error);
        return;
      }

      resolve(rows);
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

function cleanSql(sqlText) {
  let sql = sqlText.trim();

  sql = sql
    .replace(/^```(?:sql)?/i, "")
    .replace(/```$/i, "")
    .trim();
  sql = sql.replace(/;\s*$/, "").trim();

  if (!/^(select|with)\s/i.test(sql)) {
    throw new Error("Only SELECT queries are allowed");
  }

  if (sql.includes(";")) {
    throw new Error("Multiple SQL statements are not allowed");
  }

  return sql;
}

async function classifyQuestion(message, schemaSummary) {
  const prompt = [
    "Classify this question for the Order to Cash dataset.",
    'Return JSON only in the form {"scope":"in_scope"|"out_of_scope","reason":"..."}.',
    'Set scope to "out_of_scope" if the question is not about orders, delivery, billing, journal entries, payments, customers, products, plants, storage locations, or their master data.',
    "",
    "Schema:",
    schemaSummary,
    "",
    "Question:",
    message,
  ].join("\n");

  const result = parseJsonResponse(await askLLM(prompt));

  if (!result.scope) {
    throw new Error("Classifier response missing scope");
  }

  return result;
}

async function generateSql(message, schemaSummary) {
  const sql = await askSQL(message, schemaSummary);
  return cleanSql(sql);
}

async function summarizeResult(message, sql, rows) {
  const prompt = [
    "Answer the user's question using only the SQL result rows.",
    'Return JSON only in the form {"answer":"..."}.',
    "If there are no rows, answer exactly: No matching records found in the dataset.",
    "Do not invent facts or mention data not present in the result rows.",
    "",
    "Question:",
    message,
    "",
    "SQL:",
    sql,
    "",
    "Rows:",
    JSON.stringify(rows.slice(0, MAX_RESULT_ROWS), null, 2),
  ].join("\n");

  const result = parseJsonResponse(await askLLM(prompt));

  return result.answer || "No matching records found in the dataset.";
}

function collectHighlightCandidates(rows) {
  const values = new Set();

  for (const row of rows || []) {
    for (const value of Object.values(row)) {
      if (value === null || value === undefined) {
        continue;
      }

      const text = String(value).trim();

      if (text) {
        values.add(text);
      }
    }
  }

  return Array.from(values);
}

function extractQuestionIdentifiers(question) {
  return Array.from(
    new Set(
      String(question || "")
        .match(/\b\d{5,}\b/g)
        ?.map((value) => value.trim())
        .filter(Boolean) || [],
    ),
  );
}

function normalizeItemKey(value) {
  return String(value || "").replace(/^0+/, "") || "0";
}

async function directBillingLookup(question) {
  if (!/(journal\s+entry|payment|billing\s+document)/i.test(question)) {
    return null;
  }

  const { db } = await initQueryEngine();
  const identifiers = extractQuestionIdentifiers(question);

  for (const identifier of identifiers) {
    const billingRows = await promisifyAll(
      db,
      "SELECT accountingDocument AS journalEntryNumber, billingDocument AS sourceBillingDocument FROM billing_document_headers WHERE billingDocument = ? LIMIT 1",
      [identifier],
    );

    if (billingRows.length > 0) {
      return {
        sql: [
          "SELECT",
          "  bh.billingDocument,",
          "  bh.accountingDocument AS journalEntryNumber,",
          "  j.accountingDocumentItem,",
          "  j.customer,",
          "  j.glAccount,",
          "  j.amountInTransactionCurrency,",
          "  j.transactionCurrency,",
          "  p.clearingAccountingDocument AS paymentDocument,",
          "  p.clearingDate,",
          "  p.amountInTransactionCurrency AS paymentAmount,",
          "  p.transactionCurrency AS paymentCurrency",
          "FROM billing_document_headers bh",
          "LEFT JOIN journal_entry_items_accounts_receivable j",
          "  ON j.referenceDocument = bh.billingDocument",
          "LEFT JOIN payments_accounts_receivable p",
          "  ON p.customer = bh.soldToParty",
          "  AND (p.invoiceReference = bh.billingDocument OR p.salesDocument = bh.billingDocument OR p.clearingAccountingDocument = j.clearingAccountingDocument)",
          "WHERE bh.billingDocument = ?",
          "LIMIT 25",
        ].join("\n"),
        rows: billingRows,
      };
    }

    const journalRows = await promisifyAll(
      db,
      "SELECT accountingDocument AS journalEntryNumber, referenceDocument AS sourceBillingDocument FROM journal_entry_items_accounts_receivable WHERE referenceDocument = ? LIMIT 1",
      [identifier],
    );

    if (journalRows.length > 0) {
      return {
        sql: "SELECT accountingDocument AS journalEntryNumber, referenceDocument AS sourceBillingDocument FROM journal_entry_items_accounts_receivable WHERE referenceDocument = ? LIMIT 1",
        rows: journalRows,
      };
    }
  }

  return null;
}

async function directSalesOrderFlow(question) {
  if (!/sales\s+order/i.test(question)) {
    return null;
  }

  const identifiers = extractQuestionIdentifiers(question);

  if (identifiers.length === 0) {
    return null;
  }

  const { db } = await initQueryEngine();

  for (const identifier of identifiers) {
    const rows = await promisifyAll(
      db,
      [
        "SELECT",
        "  soh.salesOrder,",
        "  soi.salesOrderItem,",
        "  soi.material,",
        "  odi.deliveryDocument,",
        "  odi.deliveryDocumentItem,",
        "  bdi.billingDocument,",
        "  bdi.billingDocumentItem,",
        "  bh.accountingDocument AS journalEntryNumber,",
        "  bh.totalNetAmount,",
        "  bh.transactionCurrency",
        "FROM sales_order_headers soh",
        "LEFT JOIN sales_order_items soi",
        "  ON soi.salesOrder = soh.salesOrder",
        "LEFT JOIN outbound_delivery_items odi",
        "  ON odi.referenceSdDocument = soi.salesOrder",
        "  AND ltrim(odi.referenceSdDocumentItem, '0') = ltrim(soi.salesOrderItem, '0')",
        "LEFT JOIN billing_document_items bdi",
        "  ON bdi.referenceSdDocument = odi.deliveryDocument",
        "  AND ltrim(bdi.referenceSdDocumentItem, '0') = ltrim(odi.deliveryDocumentItem, '0')",
        "LEFT JOIN billing_document_headers bh",
        "  ON bh.billingDocument = bdi.billingDocument",
        "WHERE soh.salesOrder = ?",
        "ORDER BY CAST(soi.salesOrderItem AS INTEGER), odi.deliveryDocumentItem, bdi.billingDocumentItem",
        "LIMIT 100",
      ].join("\n"),
      [identifier],
    );

    if (rows.length > 0) {
      return {
        sql: [
          "SELECT",
          "  soh.salesOrder,",
          "  soi.salesOrderItem,",
          "  soi.material,",
          "  odi.deliveryDocument,",
          "  odi.deliveryDocumentItem,",
          "  bdi.billingDocument,",
          "  bdi.billingDocumentItem,",
          "  bh.accountingDocument AS journalEntryNumber,",
          "  bh.totalNetAmount,",
          "  bh.transactionCurrency",
          "FROM sales_order_headers soh",
          "LEFT JOIN sales_order_items soi",
          "  ON soi.salesOrder = soh.salesOrder",
          "LEFT JOIN outbound_delivery_items odi",
          "  ON odi.referenceSdDocument = soi.salesOrder",
          "  AND ltrim(odi.referenceSdDocumentItem, '0') = ltrim(soi.salesOrderItem, '0')",
          "LEFT JOIN billing_document_items bdi",
          "  ON bdi.referenceSdDocument = odi.deliveryDocument",
          "  AND ltrim(bdi.referenceSdDocumentItem, '0') = ltrim(odi.deliveryDocumentItem, '0')",
          "LEFT JOIN billing_document_headers bh",
          "  ON bh.billingDocument = bdi.billingDocument",
          "WHERE soh.salesOrder = ?",
          "ORDER BY CAST(soi.salesOrderItem AS INTEGER), odi.deliveryDocumentItem, bdi.billingDocumentItem",
          "LIMIT 100",
        ].join("\n"),
        rows,
      };
    }
  }

  return null;
}

async function initQueryEngine() {
  if (!initPromise) {
    initPromise = initDB();
  }

  return initPromise;
}

async function handleQuery(message) {
  const question = String(message || "").trim();

  if (!question) {
    return {
      answer: "Please ask a question about the order-to-cash dataset.",
      highlight: [],
    };
  }

  try {
    const { db, schemaSummary } = await initQueryEngine();

    if (/sales\s+order/i.test(question)) {
      const identifiers = extractQuestionIdentifiers(question);

      if (identifiers.length === 0) {
        return {
          answer:
            "Please include a sales order number, for example: Show the order to billing flow for sales order 740506.",
          highlight: [],
        };
      }

      const directOrderFlow = await directSalesOrderFlow(question);

      if (directOrderFlow) {
        const answer = await summarizeResult(
          question,
          directOrderFlow.sql,
          directOrderFlow.rows,
        );

        return {
          answer,
          sql: cleanSql(directOrderFlow.sql),
          rows: directOrderFlow.rows,
          highlight: collectHighlightCandidates(directOrderFlow.rows),
        };
      }

      return {
        answer: "No matching sales order flow was found for that number.",
        highlight: [],
      };
    }

    if (/(journal\s+entry|payment|billing\s+document)/i.test(question)) {
      const identifiers = extractQuestionIdentifiers(question);

      if (identifiers.length === 0) {
        return {
          answer:
            "Please include a billing document number, for example: Find journal entry or payment details for billing document 91150187.",
          highlight: [],
        };
      }

      const directLookup = await directBillingLookup(question);

      if (directLookup) {
        const answer = await summarizeResult(
          question,
          directLookup.sql,
          directLookup.rows,
        );

        return {
          answer,
          sql: cleanSql(directLookup.sql),
          rows: directLookup.rows,
          highlight: collectHighlightCandidates(directLookup.rows),
        };
      }

      return {
        answer: "No matching billing document was found for that number.",
        highlight: [],
      };
    }

    const classification = await classifyQuestion(question, schemaSummary);

    if (classification.scope === "out_of_scope") {
      return {
        answer: "This system only answers dataset queries.",
        highlight: [],
      };
    }

    const directLookup = await directBillingLookup(question);
    const directOrderFlow = directLookup
      ? null
      : await directSalesOrderFlow(question);

    if (directLookup || directOrderFlow) {
      const chosenLookup = directLookup || directOrderFlow;
      const answer = await summarizeResult(
        question,
        chosenLookup.sql,
        chosenLookup.rows,
      );

      return {
        answer,
        sql: cleanSql(chosenLookup.sql),
        rows: chosenLookup.rows,
        highlight: collectHighlightCandidates(chosenLookup.rows),
      };
    }

    const sql = await generateSql(question, schemaSummary);
    const rows = await promisifyAll(db, sql);
    const answer = await summarizeResult(question, sql, rows);
    const highlight = collectHighlightCandidates(rows);

    return {
      answer,
      sql,
      rows,
      highlight,
    };
  } catch (error) {
    const errorText = String(error.message || "");

    if (
      errorText.includes("SQLITE_ERROR") ||
      errorText.toLowerCase().includes("circular reference") ||
      errorText.toLowerCase().includes("union all")
    ) {
      return {
        answer:
          "I could not build a valid SQL query for that wording. Try asking with a specific sales order number or billing document number.",
        highlight: [],
      };
    }

    if (String(error.message || "").includes("GROQ_API_KEY")) {
      return {
        answer:
          "GROQ_API_KEY is not configured in backend/.env. Add your Groq key to enable live SQL generation.",
        highlight: [],
      };
    }

    if (
      String(error.message || "")
        .toLowerCase()
        .includes("groq request failed")
    ) {
      return {
        answer:
          "Groq request failed. Check the model name in backend/.env and the backend console for the exact API error.",
        highlight: [],
      };
    }

    throw error;
  }
}

module.exports = {
  initQueryEngine,
  handleQuery,
};
