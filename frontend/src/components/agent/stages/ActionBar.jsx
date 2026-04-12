import React, { useState } from 'react';
import { Code, Copy, Check, RefreshCw } from 'lucide-react';

/**
 * Stage 6: Action Bar — appears after streaming completes when DSL code was produced.
 * [ Insert ] [ Copy ] [ Replace ]
 *
 * compact: when true, renders smaller inline buttons (used inside code blocks).
 */
export default function ActionBar({ code, onInsert, onReplace, onCopy, compact }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    if (onCopy) onCopy(code);
    else navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className={`action-bar${compact ? ' action-bar-compact' : ''}`}>
      {onInsert && (
        <button className="action-btn primary" onClick={() => onInsert(code)}>
          <Code size={compact ? 10 : 12} /> Insert
        </button>
      )}
      <button className="action-btn" onClick={handleCopy}>
        {copied ? <><Check size={compact ? 10 : 12} /> Copied</> : <><Copy size={compact ? 10 : 12} /> Copy</>}
      </button>
      {onReplace && (
        <button className="action-btn" onClick={() => onReplace(code)}>
          <RefreshCw size={compact ? 10 : 12} /> Replace
        </button>
      )}
    </div>
  );
}
