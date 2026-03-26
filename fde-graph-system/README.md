# FDE Graph System

Order-to-cash graph explorer with a React/Vite frontend and an Express/SQLite backend.

## Run

From the project root:

```bash
npm start
```

That starts the backend on port `3001`.

To run the full stack in development mode:

```bash
npm run dev
```

That starts the backend and the frontend dev server.

To build the frontend:

```bash
npm run build
```

## Folder Layout

- `backend/` - Express API, graph builder, SQLite query engine
- `frontend/` - React graph UI and chat panel
- `data/` - SAP order-to-cash source data
