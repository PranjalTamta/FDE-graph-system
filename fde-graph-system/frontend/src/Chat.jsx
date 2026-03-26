import { useState } from "react";
import axios from "axios";

function Chat({ setHighlight }) {
  const [msg, setMsg] = useState("");
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      text: "Ask about orders, delivery, billing, payments, customers, products, or plants.",
    },
  ]);
  const [loading, setLoading] = useState(false);

  const starterQuestions = [
    "Show the order to billing flow for a sales order",
    "Find billing documents for a customer",
    "Find journal entry or payment details for a billing document",
  ];

  const send = async () => {
    const text = msg.trim();

    if (!text || loading) {
      return;
    }

    setMessages((current) => [...current, { role: "user", text }]);
    setMsg("");
    setLoading(true);

    try {
      const response = await axios.post("/query", {
        message: text,
      });

      setMessages((current) => [
        ...current,
        {
          role: "assistant",
          text: response.data.answer,
          sql: response.data.sql,
          result: response.data.rows || response.data.result,
        },
      ]);

      setHighlight(
        Array.isArray(response.data.highlight) ? response.data.highlight : [],
      );
    } catch (error) {
      const backendMessage =
        error?.response?.data?.error ||
        error?.response?.data?.answer ||
        error?.message;
      setMessages((current) => [
        ...current,
        {
          role: "assistant",
          text:
            backendMessage ||
            "Query failed. Check the backend and GROQ_API_KEY.",
        },
      ]);
      setHighlight([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <aside
      style={{
        width: "100%",
        borderLeft: "1px solid rgba(15, 23, 42, 0.1)",
        background: "linear-gradient(180deg, #ffffff 0%, #f8fafc 100%)",
        display: "flex",
        flexDirection: "column",
        boxShadow: "-16px 0 40px rgba(15, 23, 42, 0.06)",
        minWidth: 0,
      }}
    >
      <div
        style={{
          padding: "20px 18px 12px",
          borderBottom: "1px solid rgba(15, 23, 42, 0.08)",
        }}
      >
        <div style={{ fontSize: 12, color: "#64748b", marginBottom: 6 }}>
          Chat with Graph
        </div>
        <div style={{ fontSize: 18, fontWeight: 700, color: "#0f172a" }}>
          Order to Cash
        </div>
        <div style={{ marginTop: 10, display: "grid", gap: 8 }}>
          {starterQuestions.map((question) => (
            <button
              key={question}
              onClick={() => setMsg(question)}
              style={{
                textAlign: "left",
                border: "1px solid rgba(15, 23, 42, 0.1)",
                background: "#ffffff",
                borderRadius: 12,
                padding: "10px 12px",
                color: "#0f172a",
                fontSize: 13,
                cursor: "pointer",
              }}
            >
              {question}
            </button>
          ))}
        </div>
      </div>

      <div
        style={{
          flex: 1,
          overflowY: "auto",
          padding: 16,
          display: "flex",
          flexDirection: "column",
          gap: 12,
        }}
      >
        {messages.map((message, index) => (
          <div
            key={index}
            style={{
              alignSelf: message.role === "user" ? "flex-end" : "flex-start",
              maxWidth: "92%",
              background: message.role === "user" ? "#111827" : "#ffffff",
              color: message.role === "user" ? "#ffffff" : "#0f172a",
              border:
                message.role === "user"
                  ? "none"
                  : "1px solid rgba(15, 23, 42, 0.08)",
              borderRadius: 16,
              padding: "12px 14px",
              boxShadow: "0 8px 24px rgba(15, 23, 42, 0.06)",
              fontSize: 14,
              lineHeight: 1.5,
            }}
          >
            {message.sql ? (
              <div style={{ marginTop: 12, fontSize: 12, color: "#475569" }}>
                <div style={{ fontWeight: 700, marginBottom: 4 }}>SQL</div>
                <pre
                  style={{
                    margin: 0,
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                    background: "#f8fafc",
                    padding: 10,
                    borderRadius: 10,
                    border: "1px solid rgba(15, 23, 42, 0.08)",
                  }}
                >
                  {message.sql}
                </pre>

                <div style={{ fontWeight: 700, margin: "10px 0 4px" }}>
                  Result
                </div>
                <pre
                  style={{
                    margin: 0,
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                    background: "#f8fafc",
                    padding: 10,
                    borderRadius: 10,
                    border: "1px solid rgba(15, 23, 42, 0.08)",
                    maxHeight: 180,
                    overflow: "auto",
                  }}
                >
                  {JSON.stringify(message.result || [], null, 2)}
                </pre>

                <div style={{ fontWeight: 700, margin: "10px 0 4px" }}>
                  Answer
                </div>
                <div>{message.text}</div>
              </div>
            ) : (
              message.text
            )}
          </div>
        ))}
      </div>

      <div
        style={{
          padding: 16,
          borderTop: "1px solid rgba(15, 23, 42, 0.08)",
          background: "rgba(255,255,255,0.85)",
          backdropFilter: "blur(8px)",
        }}
      >
        <textarea
          value={msg}
          onChange={(event) => setMsg(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();
              send();
            }
          }}
          rows={4}
          placeholder="Ask anything about the dataset"
          style={{
            width: "100%",
            resize: "none",
            borderRadius: 14,
            border: "1px solid rgba(15, 23, 42, 0.14)",
            padding: 12,
            boxSizing: "border-box",
            font: "inherit",
            outline: "none",
            marginBottom: 12,
          }}
        />

        <button
          onClick={send}
          disabled={loading}
          style={{
            width: "100%",
            border: "none",
            borderRadius: 14,
            padding: "12px 14px",
            background: loading ? "#94a3b8" : "#0f172a",
            color: "#ffffff",
            fontWeight: 700,
            cursor: loading ? "not-allowed" : "pointer",
          }}
        >
          {loading ? "Thinking..." : "Send"}
        </button>
      </div>
    </aside>
  );
}

export default Chat;
