const express = require("express");
const cors = require("cors");
const buildGraph = require("./graphBuilder");
const { initQueryEngine, handleQuery } = require("./queryEngine");

const app = express();

app.use(cors());
app.use(express.json());

app.get("/", (req, res) => {
  res.send("Server running");
});

app.get("/graph", async (req, res) => {
  try {
    const graph = await buildGraph();
    res.json(graph);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post("/query", async (req, res) => {
  try {
    const { message } = req.body;
    const result = await handleQuery(message);
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

async function startServer() {
  try {
    await Promise.all([buildGraph(), initQueryEngine()]);

    app.listen(3001, () => {
      console.log("Server running");
    });
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
}

startServer();
