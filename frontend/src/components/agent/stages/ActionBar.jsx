import React, { useState } from 'react';
import { Code, Copy, Check, RefreshCw } from 'lucide-react';

/**
 * Stage 6: Action Bar — appears after streaming completes when DSL code was produced.
 * [ Insert ] [ Copy ] [ Replace ]
 */
export default function ActionBar({ code, onInsert, onReplace, onCopy }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    if (onCopy) onCopy(code);
    else navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="action-bar">
      {onInsert && (
        <button className="action-btn primary" onClick={() => onInsert(code)}>
          <Code size={12} /> Insert
        </button>
      )}
      <button className="action-btn" onClick={handleCopy}>
        {copied ? <><Check size={12} /> Copied</> : <><Copy size={12} /> Copy</>}
      </button>
      {onReplace && (
        <button className="action-btn" onClick={() => onReplace(code)}>
          <RefreshCw size={12} /> Replace
        </button>
      )}
    </div>
  );
}
