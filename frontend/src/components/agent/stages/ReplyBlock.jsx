import React, { useRef, useEffect } from 'react';
import ActionBar from './ActionBar';

/**
 * Stage 5: Streaming reply — renders markdown and DSL code blocks inline.
 * Shows a blinking cursor at the end while streaming.
 * Each code block gets its own Insert/Copy/Replace action bar.
 */
export default function ReplyBlock({ tokens, streaming, onInsertCode, onOverwriteCode }) {
  const endRef = useRef(null);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }, [tokens]);

  // Parse tokens into text and code blocks
  const parts = [];
  const lines = tokens.split('\n');
  let inCode = false;
  let codeBuf = [];
  let textBuf = [];

  const flushText = () => {
    if (textBuf.length > 0) {
      parts.push({ type: 'text', content: textBuf.join('\n') });
      textBuf = [];
    }
  };
  const flushCode = () => {
    if (codeBuf.length > 0) {
      parts.push({ type: 'code', content: codeBuf.join('\n') });
      codeBuf = [];
    }
  };

  for (const line of lines) {
    if (line.trim().startsWith('```')) {
      if (inCode) { flushCode(); } else { flushText(); }
      inCode = !inCode;
      continue;
    }
    if (inCode) codeBuf.push(line); else textBuf.push(line);
  }
  flushText();
  flushCode();

  // Detect structured JSON response with dsl_code and convert to readable parts
  if (parts.length >= 1) {
    const newParts = [];
    for (const part of parts) {
      if (part.type === 'code') {
        try {
          const parsed = JSON.parse(part.content);
          if (parsed && parsed.dsl_code) {
            // Add explanation as text
            if (parsed.explanation) {
              newParts.push({ type: 'text', content: parsed.explanation });
            }
            // Add dsl_code as a proper code block with unescaped newlines
            const cleanCode = parsed.dsl_code.replace(/\\n/g, '\n').replace(/\\t/g, '\t');
            newParts.push({ type: 'code', content: cleanCode });
            continue;
          }
        } catch {
          // Not JSON — render as-is
        }
      }
      newParts.push(part);
    }
    parts.length = 0;
    parts.push(...newParts);
  }

  // Render inline markdown (bold, italic, inline code)
  const renderInline = (text) => {
    const result = [];
    // Split by **bold**, *italic*, `code`
    const regex = /(\*\*.*?\*\*|\*.*?\*|`.*?`)/g;
    let lastIdx = 0;
    let match;
    while ((match = regex.exec(text)) !== null) {
      if (match.index > lastIdx) result.push(text.slice(lastIdx, match.index));
      const m = match[0];
      if (m.startsWith('**')) {
        result.push(<strong key={match.index}>{m.slice(2, -2)}</strong>);
      } else if (m.startsWith('`')) {
        result.push(<code key={match.index} className="reply-inline-code">{m.slice(1, -1)}</code>);
      } else if (m.startsWith('*')) {
        result.push(<em key={match.index}>{m.slice(1, -1)}</em>);
      }
      lastIdx = match.index + m.length;
    }
    if (lastIdx < text.length) result.push(text.slice(lastIdx));
    return result;
  };

  return (
    <div className="reply-block">
      {parts.map((part, idx) => {
        if (part.type === 'code') {
          return (
            <div key={idx} className="reply-code-block">
              <pre><code>{part.content}</code></pre>
              {!streaming && (onInsertCode || onOverwriteCode) && (
                <ActionBar
                  code={part.content}
                  onInsert={onInsertCode}
                  onReplace={onOverwriteCode}
                  compact
                />
              )}
            </div>
          );
        }
        return (
          <div key={idx} className="reply-text">
            {part.content.split('\n').map((line, li) => {
              if (!line.trim()) return <p key={li} className="reply-empty-line">&nbsp;</p>;
              const listMatch = line.match(/^[-•]\s+(.*)/);
              if (listMatch) {
                return <div key={li} className="reply-list-item">• {renderInline(listMatch[1])}</div>;
              }
              const numMatch = line.match(/^(\d+)\.\s+(.*)/);
              if (numMatch) {
                return <div key={li} className="reply-list-item">{numMatch[1]}. {renderInline(numMatch[2])}</div>;
              }
              return <p key={li} className="reply-paragraph">{renderInline(line)}</p>;
            })}
          </div>
        );
      })}
      {streaming && <span className="reply-cursor" />}
      <span ref={endRef} />
    </div>
  );
}
