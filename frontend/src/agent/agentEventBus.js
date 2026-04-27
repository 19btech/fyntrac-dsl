/**
 * Agent Event Bus — decoupled event layer between the agent pipeline and UI.
 * The pipeline emits events. The UI subscribes per messageId and renders.
 */

class AgentEventBus {
  constructor() {
    this._listeners = new Map(); // messageId → Set<callback>
    this._history = new Map();   // messageId → Array<event> (for replay)
  }

  subscribe(messageId, callback) {
    if (!this._listeners.has(messageId)) {
      this._listeners.set(messageId, new Set());
    }
    this._listeners.get(messageId).add(callback);

    // Replay any events that were emitted before this subscriber attached
    const past = this._history.get(messageId);
    if (past) {
      past.forEach((evt) => callback(evt));
    }

    return () => {
      const set = this._listeners.get(messageId);
      if (set) {
        set.delete(callback);
        if (set.size === 0) this._listeners.delete(messageId);
      }
    };
  }

  emit(messageId, event) {
    // Store event in history for late subscribers
    if (!this._history.has(messageId)) {
      this._history.set(messageId, []);
    }
    this._history.get(messageId).push(event);

    const set = this._listeners.get(messageId);
    if (set) set.forEach((cb) => cb(event));
  }

  /** Clean up history for a given messageId (call when message is fully done). */
  cleanup(messageId) {
    this._history.delete(messageId);
  }

  // ── Convenience emitters used by the agent pipeline ──
  thinking(id, text) {
    this.emit(id, { type: 'THINKING', text });
  }
  readStep(id, icon, label, result) {
    this.emit(id, { type: 'READ_STEP', icon, label, result });
  }
  planReady(id, steps) {
    this.emit(id, { type: 'PLAN_READY', steps });
  }
  execStepStart(id, stepId, label) {
    this.emit(id, { type: 'EXEC_STEP_START', id: stepId, label });
  }
  execStepDone(id, stepId, detail) {
    this.emit(id, { type: 'EXEC_STEP_COMPLETE', id: stepId, detail });
  }
  replyToken(id, token) {
    this.emit(id, { type: 'REPLY_TOKEN', token });
  }
  dslReady(id, code, insertMode) {
    this.emit(id, { type: 'DSL_READY', code, insertMode });
  }
  complete(id) {
    this.emit(id, { type: 'COMPLETE' });
  }
  error(id, message) {
    this.emit(id, { type: 'ERROR', message });
  }
}

export const agentEventBus = new AgentEventBus();
