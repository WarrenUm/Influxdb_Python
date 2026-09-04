import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import "./index.css";
import { applyThemeWithFallback } from "./theme";

// Apply the dark theme at startup. This never throws: if the dark theme fails
// to load it falls back to the default theme so the app still renders (Req 17.4).
applyThemeWithFallback();

const container = document.getElementById("root");
if (!container) {
  throw new Error('Root element "#root" not found in index.html');
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
