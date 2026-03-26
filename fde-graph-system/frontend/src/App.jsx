import { useState } from "react";
import GraphView from "./GraphView";
import Chat from "./Chat";
import "./App.css";

function App() {
  const [highlight, setHighlight] = useState([]);

  return (
    <div className="app-shell">
      <div className="graph-panel">
        <GraphView highlight={highlight} />
      </div>
      <Chat setHighlight={setHighlight} />
    </div>
  );
}

export default App;
