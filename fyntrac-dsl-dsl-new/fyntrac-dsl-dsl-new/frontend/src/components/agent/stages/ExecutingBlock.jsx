import React from 'react';

/**
 * Stage 4: Executing — live step status with progress indicator.
 * Status: 'pending' (○) | 'running' (● + bar) | 'complete' (✓) | 'failed' (✗)
 */
export default function ExecutingBlock({ steps }) {
  return (
    <div className="executing-block">
      {steps.map((step, i) => (
        <div key={step.id || i} className={`step-row ${step.status}`}>
          <span className="step-check">
            {step.status === 'complete' ? '✓' : step.status === 'running' ? '●' : step.status === 'failed' ? '✗' : '○'}
          </span>
          <span className="step-label">{step.label}</span>
          {step.status === 'running' && (
            <span className="progress-bar"><span className="progress-bar-fill" /></span>
          )}
          {step.detail && step.status === 'complete' && (
            <span className="step-result">{step.detail}</span>
          )}
        </div>
      ))}
    </div>
  );
}
