import React from 'react';

/**
 * Stage 3: Planning — numbered plan steps before execution begins.
 */
export default function PlanningBlock({ steps, active }) {
  if (!steps || steps.length === 0) return null;
  return (
    <div className={`plan-block ${active ? 'active' : ''}`}>
      <div className="plan-title">Planning</div>
      {steps.map((step, i) => (
        <div key={i} className="plan-step">
          {i + 1}. {step}
        </div>
      ))}
    </div>
  );
}
