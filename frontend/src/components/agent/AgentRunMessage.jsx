import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  CheckCircle2, AlertTriangle, Loader2, Wrench, Sparkles,
  StopCircle, ChevronDown, ChevronRight, ShieldAlert, Brain,
} from "lucide-react";
import { API } from "../../config";
import "./AgentMessage.css";

/**
 * Renders one autonomous agent run.
 * Opens a streaming POST to /api/agent/run and displays each event (thinking,
 * tool_start, tool_done, tool_error, tool_pending, final) as a timeline entry.
 *
 * Destructive tools (delete_template, clear_all_data) emit a tool_pending
 * event; we surface Approve / Deny buttons that hit
 * /api/agent/runs/{run_id}/approve.
 */
const TOOL_LABELS = {
  list_events: "Listing events",
  list_dsl_functions: "Inspecting DSL functions",
  list_templates: "Listing templates",
  create_event_definitions: "Creating event definitions",
  add_transaction_types: "Adding transaction types",
  generate_sample_event_data: "Generating sample data",
  get_event_data: "Reading event data",
  validate_dsl: "Validating DSL",
  create_or_replace_template: "Saving template",
  dry_run_template: "Dry-running template",
  delete_template: "Deleting template",
  clear_all_data: "Clearing all data",
  // Rules / steps / schedules / template assembly
  list_saved_rules: "Listing saved rules",
  get_saved_rule: "Reading rule",
  create_saved_rule: "Creating rule",
  update_saved_rule: "Updating rule",
  delete_saved_rule: "Deleting rule",
  add_step_to_rule: "Adding step",
  update_step: "Updating step",
  delete_step: "Deleting step",
  debug_step: "Debugging step",
  list_saved_schedules: "Listing saved schedules",
  create_saved_schedule: "Creating schedule",
  delete_saved_schedule: "Deleting schedule",
  debug_schedule: "Debugging schedule",
  verify_rule_complete: "Verifying rule readiness",
  attach_rules_to_template: "Assembling template",
  get_dsl_syntax_guide: "Reading DSL syntax guide",
  finish: "Finalising",
};

function shortenJSON(value, maxLen = 320) {
  let s;
  try { s = JSON.stringify(value); } catch { s = String(value); }
  if (s == null) return "";
  return s.length > maxLen ? s.slice(0, maxLen) + "…" : s;
}

const AgentRunMessage = ({ task, model, autoApproveDestructive = false, onComplete, initialEvents, initialStatus, onAgentDataChange, sessionId }) => {
  const isReplay = Array.isArray(initialEvents) && initialEvents.length > 0;
  const [events, setEvents] = useState(isReplay ? initialEvents : []);
  const eventsRef = useRef(events);
  useEffect(() => { eventsRef.current = events; }, [events]);
  const [runId, setRunId] = useState(null);
  const [status, setStatus] = useState(initialStatus || (isReplay ? "done" : "running"));
  const [errorMsg, setErrorMsg] = useState(null);
  const [pending, setPending] = useState({}); // call_id -> {name, args, decided}
  const [expanded, setExpanded] = useState({}); // call_id -> bool
  const abortRef = useRef(null);
  // Stash the latest onComplete in a ref so the streaming effect doesn't
  // need it in its dependency array. Otherwise every parent re-render
  // (which passes a new arrow function) would tear down the SSE stream.
  const onCompleteRef = useRef(onComplete);
  useEffect(() => { onCompleteRef.current = onComplete; }, [onComplete]);
  const onAgentDataChangeRef = useRef(onAgentDataChange);
  useEffect(() => { onAgentDataChangeRef.current = onAgentDataChange; }, [onAgentDataChange]);

  // Tools that mutate server-side state — fire UI refresh after they succeed.
  const MUTATING_TOOLS = useRef(new Set([
    "create_event_definitions", "add_transaction_types",
    "generate_sample_event_data", "create_or_replace_template",
    "delete_template", "clear_all_data",
    // Rule / step / schedule / template assembly
    "create_saved_rule", "update_saved_rule", "delete_saved_rule",
    "add_step_to_rule", "update_step", "delete_step",
    "create_saved_schedule", "delete_saved_schedule",
    "attach_rules_to_template",
  ])).current;

  // Timeline derivation
  const timeline = useMemo(() => {
    // Each tool call is one row; we merge tool_start / tool_done / tool_error
    // by call_id. Plain text/thinking events are interleaved.
    const rows = [];
    const callIndex = {};
    for (const ev of events) {
      if (ev.type === "tool_start" || ev.type === "tool_pending") {
        const idx = callIndex[ev.call_id];
        if (idx == null) {
          callIndex[ev.call_id] = rows.length;
          rows.push({
            kind: "tool",
            call_id: ev.call_id,
            name: ev.name,
            args: ev.args,
            state: ev.type === "tool_pending" ? "pending" : "running",
            result: null,
            error: null,
            duration_ms: null,
          });
        } else {
          rows[idx].args = ev.args;
          rows[idx].state = ev.type === "tool_pending" ? "pending" : "running";
        }
      } else if (ev.type === "tool_done") {
        const idx = callIndex[ev.call_id];
        if (idx != null) {
          rows[idx].state = "done";
          rows[idx].result = ev.result;
          rows[idx].duration_ms = ev.duration_ms;
        }
      } else if (ev.type === "tool_error") {
        const idx = callIndex[ev.call_id];
        if (idx != null) {
          rows[idx].state = "error";
          rows[idx].error = ev.error;
          rows[idx].duration_ms = ev.duration_ms;
        } else {
          rows.push({ kind: "tool", call_id: ev.call_id, name: ev.name,
                       args: {}, state: "error", error: ev.error });
        }
      } else if (ev.type === "assistant_message" && ev.content) {
        rows.push({ kind: "text", content: ev.content, step: ev.step });
      } else if (ev.type === "thinking") {
        // skip rendering raw thinking ticks; they'd be noisy
      } else if (ev.type === "calling_model") {
        // Replace any prior calling_model row for the same step so the
        // heartbeat updates in place rather than spamming the timeline.
        const lastIdx = rows.length - 1;
        if (lastIdx >= 0 && rows[lastIdx].kind === "calling" && rows[lastIdx].step === ev.step) {
          rows[lastIdx] = { kind: "calling", step: ev.step, model: ev.model, elapsed_s: 0 };
        } else {
          rows.push({ kind: "calling", step: ev.step, model: ev.model, elapsed_s: 0 });
        }
      } else if (ev.type === "heartbeat") {
        // Update the most recent "calling" row for this step.
        for (let i = rows.length - 1; i >= 0; i--) {
          if (rows[i].kind === "calling" && rows[i].step === ev.step) {
            rows[i] = { ...rows[i], elapsed_s: ev.elapsed_s };
            break;
          }
        }
      } else if (ev.type === "warning") {
        rows.push({ kind: "warning", content: ev.message });
      } else if (ev.type === "final") {
        rows.push({ kind: "final", status: ev.status,
                     summary: ev.summary, steps: ev.steps });
      } else if (ev.type === "error") {
        rows.push({ kind: "error", content: ev.error_message || ev.message });
      } else if (ev.type === "run_started") {
        rows.push({ kind: "started", model: ev.model, max_steps: ev.max_steps });
      }
    }
    return rows;
  }, [events]);

  useEffect(() => {
    if (isReplay) return; // historical run from localStorage — don't re-stream
    let cancelled = false;
    const ctrl = new AbortController();
    abortRef.current = ctrl;

    (async () => {
      try {
        const resp = await fetch(`${API}/agent/run`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            task,
            model: model || undefined,
            auto_approve_destructive: autoApproveDestructive,
            session_id: sessionId || undefined,
          }),
          signal: ctrl.signal,
        });
        if (!resp.ok || !resp.body) {
          throw new Error(`HTTP ${resp.status}`);
        }
        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        while (!cancelled) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let nlIdx;
          while ((nlIdx = buffer.indexOf("\n\n")) >= 0) {
            const chunk = buffer.slice(0, nlIdx);
            buffer = buffer.slice(nlIdx + 2);
            for (const line of chunk.split("\n")) {
              if (!line.startsWith("data:")) continue;
              const payload = line.slice(5).trim();
              if (payload === "[DONE]") {
                setStatus(prev => (prev === "running" ? "done" : prev));
                continue;
              }
              try {
                const ev = JSON.parse(payload);
                if (ev.type === "run_started" && ev.run_id) {
                  setRunId(ev.run_id);
                }
                if (ev.type === "tool_pending") {
                  setPending(p => ({ ...p,
                    [ev.call_id]: { name: ev.name, args: ev.args, decided: false } }));
                }
                if (ev.type === "tool_done" || ev.type === "tool_error") {
                  setPending(p => {
                    if (!p[ev.call_id]) return p;
                    const next = { ...p };
                    delete next[ev.call_id];
                    return next;
                  });
                  if (ev.type === "tool_done" && MUTATING_TOOLS.has(ev.name)) {
                    try { onAgentDataChangeRef.current?.(ev.name, ev); } catch (_) {}
                  }
                }
                if (ev.type === "final") {
                  setStatus(ev.status === "completed" ? "done"
                              : ev.status === "cancelled" ? "cancelled" : "done");
                  // Pass the full event log so parent can persist the run.
                  const snapshot = [...eventsRef.current, ev];
                  onCompleteRef.current?.(ev, snapshot);
                  // Final UI refresh in case any mutations happened.
                  try { onAgentDataChangeRef.current?.("final"); } catch (_) {}
                }
                if (ev.type === "error") {
                  setStatus("error");
                  setErrorMsg(ev.error_message || ev.message || "Agent error");
                }
                setEvents(prev => [...prev, ev]);
              } catch (e) {
                // ignore malformed
              }
            }
          }
        }
      } catch (err) {
        if (err.name !== "AbortError") {
          setStatus("error");
          setErrorMsg(err.message || String(err));
        }
      }
    })();

    return () => {
      cancelled = true;
      try { ctrl.abort(); } catch (_) {}
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleApprove = async (callId, decision) => {
    if (!runId) return;
    setPending(p => p[callId] ? { ...p, [callId]: { ...p[callId], decided: true } } : p);
    try {
      await fetch(`${API}/agent/runs/${runId}/approve?call_id=${encodeURIComponent(callId)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ decision }),
      });
    } catch (e) {
      // ignore — runtime will time out and handle
    }
  };

  const handleStop = async () => {
    // Always abort the local stream first so the UI unblocks immediately,
    // even if we never received a run_started event from the server.
    try { abortRef.current?.abort(); } catch (_) {}
    setStatus("cancelled");
    if (runId) {
      try {
        await fetch(`${API}/agent/runs/${runId}/cancel`, { method: "POST" });
      } catch (_) {}
    }
    // Notify parent so the typing indicator/disabled input clears.
    try {
      onCompleteRef.current?.({ type: "final", status: "cancelled",
                      summary: "Stopped by user.", steps: 0 });
    } catch (_) {}
  };

  const toggle = (id) => setExpanded(e => ({ ...e, [id]: !e[id] }));

  return (
    <div className="agent-run">
      <div className="agent-run-header">
        <div className="agent-run-header-left">
          {status === "running" ? <Loader2 size={14} className="spin" />
            : status === "error" ? <AlertTriangle size={14} className="err" />
            : status === "cancelled" ? <StopCircle size={14} />
            : <CheckCircle2 size={14} className="ok" />}
          <span className="agent-run-title">
            Agent {status === "running" ? "working" : status}
          </span>
        </div>
        {status === "running" && (
          <button className="agent-run-stop" onClick={handleStop} title="Stop run">
            <StopCircle size={12} /> Stop
          </button>
        )}
      </div>

      {errorMsg && (
        <div className="agent-run-error">
          <AlertTriangle size={12} /> {errorMsg}
        </div>
      )}

      <div className="agent-run-timeline">
        {timeline.length === 0 && status === "running" && (
          <div className="agent-run-row meta">
            <Loader2 size={12} className="spin" />
            <span>Connecting to agent runtime…</span>
          </div>
        )}
        {timeline.map((row, idx) => {
          if (row.kind === "started") {
            return (
              <div key={idx} className="agent-run-row meta">
                <Sparkles size={12} /> Using {row.model || "default model"} · max {row.max_steps} steps
              </div>
            );
          }
          if (row.kind === "text") {
            return (
              <div key={idx} className="agent-run-row text">
                <Brain size={12} className="muted" />
                <span>{row.content}</span>
              </div>
            );
          }
          if (row.kind === "calling") {
            // Only spin while the run is still active AND this is the most
            // recent calling row. Older steps' calls are already finished.
            const isLatestCalling = (() => {
              for (let i = timeline.length - 1; i >= 0; i--) {
                if (timeline[i].kind === "calling") return i === idx;
              }
              return false;
            })();
            const stillThinking = status === "running" && isLatestCalling;
            return (
              <div key={idx} className="agent-run-row meta">
                {stillThinking
                  ? <Loader2 size={12} className="spin" />
                  : <CheckCircle2 size={12} className="ok" />}
                <span>
                  Step {row.step}: thinking with {row.model || "model"}
                  {row.elapsed_s > 0 ? ` · ${row.elapsed_s}s` : ""}
                  {stillThinking ? "…" : ""}
                </span>
              </div>
            );
          }
          if (row.kind === "warning") {
            return (
              <div key={idx} className="agent-run-row warn">
                <AlertTriangle size={12} /> {row.content}
              </div>
            );
          }
          if (row.kind === "error") {
            return (
              <div key={idx} className="agent-run-row error">
                <AlertTriangle size={12} /> {row.content}
              </div>
            );
          }
          if (row.kind === "final") {
            return (
              <div key={idx} className={`agent-run-row final ${row.status}`}>
                <CheckCircle2 size={12} /> <strong>{row.status}</strong> ·{" "}
                {row.summary} <em>({row.steps} steps)</em>
              </div>
            );
          }
          if (row.kind === "tool") {
            const label = TOOL_LABELS[row.name] || row.name;
            const isOpen = !!expanded[row.call_id];
            const stateIcon = row.state === "running" ? <Loader2 size={12} className="spin" />
              : row.state === "done" ? <CheckCircle2 size={12} className="ok" />
              : row.state === "error" ? <AlertTriangle size={12} className="err" />
              : row.state === "pending" ? <ShieldAlert size={12} className="warn" />
              : <Wrench size={12} />;
            const pendingEntry = pending[row.call_id];
            return (
              <div key={idx} className={`agent-run-row tool state-${row.state}`}>
                <button className="agent-run-tool-head" onClick={() => toggle(row.call_id)}>
                  {isOpen ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                  {stateIcon}
                  <span className="tool-label">{label}</span>
                  {row.duration_ms != null && (
                    <span className="tool-duration">{row.duration_ms}ms</span>
                  )}
                </button>
                {isOpen && (
                  <div className="agent-run-tool-body">
                    <div className="kv">
                      <span className="k">tool</span>
                      <code className="v">{row.name}</code>
                    </div>
                    <div className="kv">
                      <span className="k">args</span>
                      <code className="v">{shortenJSON(row.args)}</code>
                    </div>
                    {row.result != null && (
                      <div className="kv">
                        <span className="k">result</span>
                        <code className="v">{shortenJSON(row.result, 1200)}</code>
                      </div>
                    )}
                    {row.error && (
                      <div className="kv error">
                        <span className="k">error</span>
                        <code className="v">{row.error}</code>
                      </div>
                    )}
                  </div>
                )}
                {row.state === "pending" && pendingEntry && !pendingEntry.decided && (
                  <div className="agent-run-approval">
                    <ShieldAlert size={12} />
                    <span>This is a destructive action. Approve?</span>
                    <button className="approve" onClick={() => handleApprove(row.call_id, "approve")}>
                      Approve
                    </button>
                    <button className="deny" onClick={() => handleApprove(row.call_id, "deny")}>
                      Deny
                    </button>
                  </div>
                )}
              </div>
            );
          }
          return null;
        })}
      </div>
    </div>
  );
};

export default AgentRunMessage;
