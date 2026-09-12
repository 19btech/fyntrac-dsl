/**
 * Translate raw provider / tool / network errors into plain English for
 * finance & accounting users (not developers). Returns { message, technical }
 * where `technical` is true when the original was jargon we've hidden — the UI
 * can then offer a "Details" toggle for the raw text.
 */
export function friendlyError(raw) {
  // Strip an internal "Provider error:" prefix the runtime adds.
  const original = String(raw || "").replace(/^\s*provider error:\s*/i, "").trim();
  const s = original.toLowerCase();
  if (!s) return { message: "Something went wrong. Please try again.", technical: false };

  const F = (message) => ({ message, technical: true });

  // Usage / rate limits / quota
  if (s.includes("rate limit") || s.includes(" 429") || s.includes("resource_exhausted") ||
      s.includes("quota") || s.includes("insufficient_quota") || s.includes("billing")) {
    return F("The AI model is busy or its usage limit was reached. Wait a moment and try again, or switch to another model in the dropdown below.");
  }
  // Auth / key — PRECISE signals only. Do NOT include the bare word "invalid"
  // (that matches invalid_request_error, a request problem, not a key problem)
  // or "permission"/403 (that's a model-access problem, handled below).
  if (s.includes("api key was not accepted") || s.includes("api key not valid") ||
      s.includes("invalid api key") || s.includes("invalid x-api-key") ||
      s.includes("incorrect api key") || s.includes("authentication_error") ||
      s.includes("unauthenticated") || s.includes(" 401")) {
    return F("The AI provider didn't accept its access key. Please check the provider settings under Settings → AI Agent Setup.");
  }
  // Model access / not enabled — key is fine, but no access to THIS model.
  if (s.includes("permission") || s.includes(" 403") || s.includes("does not have access") ||
      s.includes("doesn't have access") || s.includes("not enabled") ||
      s.includes("service_disabled") || s.includes("model tier")) {
    return F("Your AI account doesn't have access to the selected model. Pick a different model in the dropdown below, or check your provider account.");
  }
  // Timeouts
  if (s.includes("timeout") || s.includes("timed out") || s.includes("deadline")) {
    return F("The AI model took too long to respond. Please try again.");
  }
  // Network / availability
  if (s.includes("network") || s.includes("connection") || s.includes("could not reach") ||
      s.includes("unavailable") || s.includes(" 503") || s.includes("econn")) {
    return F("Couldn't reach the AI model. Check your internet connection and try again.");
  }
  // Gemini thinking-model tool signature / function-call plumbing
  if (s.includes("thought_signature") || s.includes("functioncall") ||
      (s.includes("function call") && s.includes("missing"))) {
    return F("The AI model had trouble using its tools on this step. Please try again — if it keeps happening, switch to another model in the dropdown below.");
  }
  // Unsupported parameter (e.g. temperature on reasoning models)
  if (s.includes("unsupported") && (s.includes("temperature") || s.includes("parameter"))) {
    return F("This model doesn't support one of the settings the app used. Please try again, or pick a different model.");
  }
  // Safety / content blocks
  if (s.includes("safety") || s.includes("blocked") || s.includes("recitation")) {
    return F("The AI model declined this request. Try rephrasing what you asked for.");
  }
  // Context length
  if ((s.includes("context") || s.includes("token")) &&
      (s.includes("length") || s.includes("exceed") || s.includes("too long") || s.includes("maximum"))) {
    return F("This conversation became too long for the model to handle. Start a new chat and try again.");
  }
  // Model not found / deprecated
  if (s.includes("model_not_found") || s.includes("does not exist") || s.includes("not found") ||
      s.includes("deprecated")) {
    return F("The selected AI model isn't available. Please choose another model in the dropdown below.");
  }
  // Bad request / invalid argument — generic model plumbing problem
  if (s.includes("invalid_argument") || s.includes("invalid_request_error") ||
      s.includes("invalid_request") || s.includes(" 400") || s.includes("bad request")) {
    return F("The AI model rejected this step. Please try again — switching to another model often helps.");
  }
  // A ToolError with a clear, already-readable sentence: surface it lightly
  // cleaned rather than hiding it (these are usually actionable).
  if (original.length < 240 && !/[{}\[\]]|error code|traceback|exception/i.test(original)) {
    return { message: original, technical: false };
  }
  // Anything else technical → hide it behind a friendly line.
  return F("The AI model ran into a problem finishing this step. Please try again, or switch to another model in the dropdown below.");
}
