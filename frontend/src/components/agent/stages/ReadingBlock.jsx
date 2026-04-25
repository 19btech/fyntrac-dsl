import React from 'react';

/**
 * Stage 2: Reading — shows each read step as it completes.
 * Steps appear one by one with status indicators.
 */
export default function ReadingBlock({ steps, active }) {
  return (
    <div className={`reading-block ${active ? 'active' : ''}`}>
      {steps.map((step, i) => (
        <div
          key={i}
          className={`step-row ${step.status}`}
          style={{ animationDelay: `${i * 60}ms` }}
        >
          <span className="step-icon">{step.icon || '●'}</span>
          <span className="step-label">{step.label}</span>
          <span className="step-result">{step.result}</span>
          <span className="step-check">
            {step.status === 'complete' ? '✓' : step.status === 'failed' ? '✗' : ''}
          </span>
        </div>
      ))}
    </div>
  );
}
