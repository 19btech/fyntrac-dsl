import React, { useState, useEffect } from 'react';
import { agentEventBus } from '../../agent/agentEventBus';
import ThinkingBlock from './stages/ThinkingBlock';
import ReadingBlock from './stages/ReadingBlock';
import PlanningBlock from './stages/PlanningBlock';
import ExecutingBlock from './stages/ExecutingBlock';
import ReplyBlock from './stages/ReplyBlock';
import ActionBar from './stages/ActionBar';
import './AgentMessage.css';

const STAGES = {
  THINKING: 'thinking',
  READING: 'reading',
  PLANNING: 'planning',
  EXECUTING: 'executing',
  REPLYING: 'replying',
  COMPLETE: 'complete',
  ERROR: 'error',
};

/**
 * AgentMessage — renders a single agent response through all pipeline stages.
 * State-machine component: each stage transitions to the next as events arrive.
 */
export default function AgentMessage({ messageId, onInsertCode, onOverwriteCode }) {
  const [stage, setStage] = useState(STAGES.THINKING);
  const [thinkingText, setThinkingText] = useState('');
  const [readSteps, setReadSteps] = useState([]);
  const [planSteps, setPlanSteps] = useState([]);
  const [execSteps, setExecSteps] = useState([]);
  const [replyTokens, setReplyTokens] = useState('');
  const [dslCode, setDslCode] = useState(null);
  const [errorMessage, setErrorMessage] = useState(null);

  useEffect(() => {
    const unsub = agentEventBus.subscribe(messageId, (event) => {
      switch (event.type) {
        case 'THINKING':
          setStage(STAGES.THINKING);
          setThinkingText(event.text);
          break;

        case 'READ_STEP':
          setStage(STAGES.READING);
          setReadSteps((prev) => [...prev, { ...event, status: 'complete' }]);
          break;

        case 'PLAN_READY':
          setStage(STAGES.PLANNING);
          setPlanSteps(event.steps);
          break;

        case 'EXEC_STEP_START':
          setStage(STAGES.EXECUTING);
          setExecSteps((prev) => [...prev, { id: event.id, label: event.label, status: 'running' }]);
          break;

        case 'EXEC_STEP_COMPLETE':
          setExecSteps((prev) =>
            prev.map((s) => (s.id === event.id ? { ...s, status: 'complete', detail: event.detail } : s))
          );
          break;

        case 'REPLY_TOKEN':
          setStage(STAGES.REPLYING);
          setReplyTokens((prev) => prev + event.token);
          break;

        case 'DSL_READY':
          setDslCode(event.code);
          break;

        case 'COMPLETE':
          setStage(STAGES.COMPLETE);
          break;

        case 'ERROR':
          setStage(STAGES.ERROR);
          setErrorMessage(event.message);
          break;

        default:
          break;
      }
    });
    return unsub;
  }, [messageId]);

  return (
    <div className="agent-message">
      {/* STAGE 1 — THINKING */}
      {thinkingText && (
        <ThinkingBlock text={thinkingText} active={stage === STAGES.THINKING} />
      )}

      {/* STAGE 2 — READING */}
      {readSteps.length > 0 && (
        <ReadingBlock steps={readSteps} active={stage === STAGES.READING} />
      )}

      {/* STAGE 3 — PLANNING */}
      {planSteps.length > 0 && (
        <PlanningBlock steps={planSteps} active={stage === STAGES.PLANNING} />
      )}

      {/* STAGE 4 — EXECUTING */}
      {execSteps.length > 0 && (
        <ExecutingBlock steps={execSteps} />
      )}

      {/* STAGE 5 — STREAMING REPLY */}
      {replyTokens && (
        <ReplyBlock tokens={replyTokens} streaming={stage === STAGES.REPLYING} />
      )}

      {/* ERROR */}
      {stage === STAGES.ERROR && errorMessage && (
        <div className="agent-error-block">
          <span className="agent-error-icon">⚠</span>
          <span>{errorMessage}</span>
        </div>
      )}

      {/* STAGE 6 — ACTION BAR */}
      {stage === STAGES.COMPLETE && dslCode && (
        <ActionBar
          code={dslCode}
          onInsert={onInsertCode}
          onReplace={onOverwriteCode}
        />
      )}
    </div>
  );
}
