require("dotenv").config();

const fetch = require("node-fetch");

const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.3-70b-versatile";

const SYSTEM_PROMPT =
  "You are a data assistant for an Order to Cash SAP dataset. Only answer about orders, deliveries, billing, journal entries, payments, customers, products, plants, storage locations, and related master data. If the question is outside this dataset, reply exactly: This system only answers dataset queries. Never invent records.";

async function askSQL(question, schemaSummary = "") {
  if (
    !process.env.GROQ_API_KEY ||
    process.env.GROQ_API_KEY === "your_key_here"
  ) {
    throw new Error("GROQ_API_KEY is not configured");
  }

  const prompt = [
    "You are SQL generator.",
    "Only answer about the order-to-cash dataset.",
    "Return SQL only. No text.",
    "Use only SELECT or WITH queries.",
    "Do not invent tables or columns.",
    "",
    schemaSummary ? `Schema:\n${schemaSummary}` : "",
    "Question:",
    question,
  ]
    .filter(Boolean)
    .join("\n");

  const response = await fetch(
    "https://api.groq.com/openai/v1/chat/completions",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.GROQ_API_KEY}`,
      },
      body: JSON.stringify({
        model: GROQ_MODEL,
        temperature: 0,
        messages: [
          {
            role: "system",
            content: prompt,
          },
        ],
      }),
    },
  );

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Groq request failed: ${response.status} ${errorText}`);
  }

  const data = await response.json();
  const content = data?.choices?.[0]?.message?.content;

  if (!content) {
    throw new Error("Groq returned an empty response");
  }

  return content.trim();
}

async function askLLM(prompt) {
  if (
    !process.env.GROQ_API_KEY ||
    process.env.GROQ_API_KEY === "your_key_here"
  ) {
    throw new Error("GROQ_API_KEY is not configured");
  }

  const response = await fetch(
    "https://api.groq.com/openai/v1/chat/completions",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.GROQ_API_KEY}`,
      },
      body: JSON.stringify({
        model: GROQ_MODEL,
        temperature: 0,
        messages: [
          {
            role: "system",
            content: SYSTEM_PROMPT,
          },
          {
            role: "user",
            content: prompt,
          },
        ],
      }),
    },
  );

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Groq request failed: ${response.status} ${errorText}`);
  }

  const data = await response.json();
  const content = data?.choices?.[0]?.message?.content;

  if (!content) {
    throw new Error("Groq returned an empty response");
  }

  return content.trim();
}

module.exports = askLLM;
module.exports.askLLM = askLLM;
module.exports.askSQL = askSQL;
