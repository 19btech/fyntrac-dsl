import React from 'react';

/**
 * Stage 1: Thinking — pulsing dot + thinking text.
 * Collapses to a single muted line when no longer active.
 */
export default function ThinkingBlock({ text, active }) {
  return (
    <div className={`thinking-block ${active ? 'active' : 'inactive'}`}>
      <span className="dot" />
      <div className="thinking-content">
        <span className="thinking-label">Thinking...</span>
        <span className="thinking-text">{text}</span>
      </div>
    </div>
  );
}
