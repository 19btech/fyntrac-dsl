import React, { useState, useRef, useEffect, useCallback } from "react";
import { useToast } from "./ToastProvider";
import { Send, RotateCcw, AlertTriangle, Square, Sparkles, Bot, MessageSquare, PanelRightClose, PanelRightOpen } from "lucide-react";
import ModelSelector from "./ModelSelector";
import AgentMessage from "./agent/AgentMessage";
import AgentRunMessage from "./agent/AgentRunMessage";
import { runAgentPipeline, generateMessageId } from "../agent/agentPipeline";
import { detectFunctionMention, getExplanation, formatForChat, detectConceptMention, getConcept, formatConceptForChat } from "../agent/testing/explanationStore";
import "./ChatAssistant.css";

const ChatAssistantComponent = ({ dslFunctions, events, onInsertCode, onOverwriteCode, editorCode, consoleOutput, editorRef, monacoRef, providerRefreshKey, uiContext, onAgentDataChange, collapsed = false, onToggleCollapsed }, ref) => {
  const toast = useToast();

  const [messages, setMessages] = useState(() => {
    try {
      const saved = localStorage.getItem('chatMessages');
      if (saved) {
        const parsed = JSON.parse(saved);
        return parsed.filter(m =>
          m.role === 'user'
          || (m.role === 'assistant' && m.content)
          || (m.role === 'agent-run' && m.task)
        );
      }
      return [];
    } catch (e) {
      return [];
    }
  });
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [sessionId, setSessionId] = useState(() => {
    try {
      return localStorage.getItem('chatSessionId') || null;
    } catch (e) {
      return null;
    }
  });
  const [selectedModel, setSelectedModel] = useState("");
  const [agentMode, setAgentMode] = useState(() => {
    try { return localStorage.getItem('chatAgentMode') === '1'; } catch (e) { return false; }
  });
  const scrollRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => {
    try { localStorage.setItem('chatAgentMode', agentMode ? '1' : '0'); } catch (e) { /* ignore */ }
  }, [agentMode]);

  const handleModelChange = useCallback((model) => {
    setSelectedModel(model);
  }, []);

  React.useImperativeHandle(ref, () => ({
    clearChat: () => {
      setMessages([]);
      setSessionId(null);
      try {
        localStorage.removeItem('chatMessages');
        localStorage.removeItem('chatSessionId');
      } catch (e) { /* ignore */ }
    },
    sendMessage: (message) => {
      if (message.trim()) {
        setMessages(prev => [...prev, { role: "user", content: message }]);
        handleSendWithMessage(message);
      }
    },
    // Silent variant used by the Ask AI button: no user bubble is shown.
    // funcName is the display name (e.g. "rate"); message is the full prompt.
    sendSilentMessage: (funcName, message) => {
      if (!message.trim()) return;
      // Show a loading agent placeholder immediately
      const messageId = generateMessageId();
      setLoading(true);
      setMessages(prev => [...prev, { role: 'agent', messageId }]);
      const heading = `**How does ${funcName}() function work in Fyntrac DSL?**\n\n`;
      runAgentPipeline(message, {
        messageId,
        events: events || [],
        editorCode: editorCode || '',
        consoleOutput: consoleOutput || [],
        dslFunctions: dslFunctions || [],
        editorRef,
        monacoRef,
        selectedModel: selectedModel || undefined,
        sessionId,
        uiContext: uiContext || null,
        history: messages
          .filter(m => m.role === 'user' || (m.role === 'assistant' && m.content))
          .slice(-10)
          .map(m => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content })),
      }).then(result => {
        if (result.fullText) {
          setMessages(prev => [
            ...prev,
            { role: 'assistant', content: heading + result.fullText, _hidden: true },
          ]);
        }
        if (result.sessionId && result.sessionId !== sessionId) {
          setSessionId(result.sessionId);
        }
      }).catch(() => {
        toast.error("Failed to get response from AI assistant");
      }).finally(() => {
        setLoading(false);
      });
    },
  }));

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  useEffect(() => {
    if (!loading) return;
    const interval = setInterval(() => {
      if (scrollRef.current) {
        scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
      }
    }, 150);
    return () => clearInterval(interval);
  }, [loading]);

  useEffect(() => {
    try {
      const persistable = messages.filter(m =>
        m.role === 'user'
        || (m.role === 'assistant' && m.content)
        || (m.role === 'agent-run' && m.task)
      );
      localStorage.setItem('chatMessages', JSON.stringify(persistable));
      if (sessionId) localStorage.setItem('chatSessionId', sessionId);
      else localStorage.removeItem('chatSessionId');
    } catch (e) { /* ignore */ }
  }, [messages, sessionId]);

  const handleSendWithMessage = async (userMessage) => {
    setLoading(true);

    // Agent mode: spawn an autonomous run instead of the explanation pipeline.
    if (agentMode) {
      const runKey = generateMessageId();
      setMessages(prev => [...prev, { role: 'agent-run', runKey, task: userMessage, model: selectedModel || undefined }]);
      // The AgentRunMessage component manages its own lifecycle; we just
      // need to clear the typing indicator once the SSE stream resolves.
      // It calls onComplete; we wire that below in render.
      return;
    }

    // Check if the user is asking about a known DSL function.
    // If we have a pre-built explanation, inject it as an instant response
    // before the AI pipeline (which provides richer, contextual answers).
    const functionName = detectFunctionMention(userMessage);
    const explanation = functionName ? getExplanation(functionName) : null;
    if (explanation) {
      const formatted = formatForChat(explanation);
      setMessages(prev => [...prev, { role: 'assistant', content: formatted }]);
    }

    // Same idea for UI concepts (Rule Builder, Saved Rules, Live Preview, etc.).
    const conceptKey = detectConceptMention(userMessage);
    const concept = conceptKey ? getConcept(conceptKey) : null;
    if (concept) {
      const formattedConcept = formatConceptForChat(concept);
      setMessages(prev => [...prev, { role: 'assistant', content: formattedConcept }]);
    }

    const messageId = generateMessageId();
    setMessages(prev => [...prev, { role: 'agent', messageId }]);

    try {
      const result = await runAgentPipeline(userMessage, {
        messageId,
        events: events || [],
        editorCode: editorCode || '',
        consoleOutput: consoleOutput || [],
        dslFunctions: dslFunctions || [],
        editorRef,
        monacoRef,
        selectedModel: selectedModel || undefined,
        sessionId,
        uiContext: uiContext || null,
        history: messages
          .filter(m => m.role === 'user' || (m.role === 'assistant' && m.content))
          .slice(-10)
          .map(m => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content })),
      });

      if (result.fullText) {
        setMessages(prev => [
          ...prev,
          { role: 'assistant', content: result.fullText, _hidden: true },
        ]);
      }

      if (result.sessionId && result.sessionId !== sessionId) {
        setSessionId(result.sessionId);
      }
    } catch (error) {
      toast.error("Failed to get response from AI assistant");
    } finally {
      setLoading(false);
    }
  };

  const handleSendMessage = async () => {
    if (!input.trim() || loading) return;
    const userMessage = input.trim();
    setInput("");
    setMessages(prev => [...prev, { role: "user", content: userMessage }]);
    await handleSendWithMessage(userMessage);
  };

  const handleClearChat = () => {
    if (loading) return;
    setMessages([]);
    setSessionId(null);
    try {
      localStorage.removeItem('chatMessages');
      localStorage.removeItem('chatSessionId');
    } catch (e) { /* ignore */ }
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  return (
    collapsed ? (
      <div
        className="vsc-chat vsc-chat-collapsed"
        data-testid="chat-assistant-collapsed"
        style={{
          width: 44,
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          padding: '8px 0',
          gap: 8,
          transition: 'width 200ms ease',
        }}
      >
        <button
          className="vsc-icon-btn"
          onClick={onToggleCollapsed}
          title="Expand AI Assistant"
          aria-label="Expand AI Assistant"
        >
          <PanelRightOpen size={18} />
        </button>
        <div title="AI Assistant" style={{ color: '#6C757D', marginTop: 4 }}>
          <Sparkles size={18} />
        </div>
        {messages.filter(m => !m._hidden).length > 0 && (
          <div
            title={`${messages.filter(m => !m._hidden).length} message(s)`}
            style={{ color: '#6C757D', marginTop: 4 }}
          >
            <MessageSquare size={16} />
          </div>
        )}
      </div>
    ) : (
    <div className="vsc-chat" data-testid="chat-assistant" style={{ transition: 'width 200ms ease' }}>
      {/* Header - Fyntrac style */}
      <div className="vsc-chat-header">
        <div className="vsc-header-left">
          <div className="vsc-header-logo">
            <Sparkles className="vsc-header-logo-icon" />
          </div>
          <span className="vsc-chat-title">AI Assistant</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          {messages.filter(m => !m._hidden).length > 0 && (
            <button className="vsc-icon-btn" onClick={handleClearChat} title="New conversation" disabled={loading}>
              <RotateCcw size={14} />
            </button>
          )}
          {onToggleCollapsed && (
            <button
              className="vsc-icon-btn"
              onClick={onToggleCollapsed}
              title="Collapse AI Assistant"
              aria-label="Collapse AI Assistant"
            >
              <PanelRightClose size={14} />
            </button>
          )}
        </div>
      </div>

      {/* Messages */}
      <div className="vsc-chat-messages" ref={scrollRef}>
        {/* Empty state */}
        {messages.filter(m => !m._hidden).length === 0 && (
          <div className="vsc-empty-state">
            <p className="vsc-empty-title">
              {agentMode ? "What should the agent build?" : "How can I help with your calculations?"}
            </p>
            <p className="vsc-empty-subtitle">
              {agentMode
                ? "I'll define events, generate sample data, write rules with DSL functions only (no custom code), then dry-run and self-debug. Destructive actions need your approval."
                : "I explain DSL functions with worked examples and walk you through the Rule Builder step by step. For full code generation, use the AI Rule Generator inside the Rule Builder."}
            </p>
            <div className="vsc-suggestions">
              {(agentMode ? [
                "Build IFRS9 ECL stage 1/2/3 with sample data for 5 loans",
                "Create an amortization rule for fixed-rate loans and verify totals",
                "Generate sample data for the Loan event and dry-run my latest template",
              ] : [
                "What does pmt() do? Show me with sample numbers.",
                "Walk me through building a loan amortization rule",
                "How do I add a Schedule step in the Rule Builder?",
              ]).map((q, i) => (
                <button key={i} className="vsc-suggestion-btn" onClick={() => setInput(q)}>
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Message list */}
        {messages.filter(m => !m._hidden).map((msg, idx) => {
          if (msg.role === 'user') {
            return (
              <div key={idx} className="vsc-msg vsc-msg-user">
                <div className="vsc-msg-content">{msg.content}</div>
              </div>
            );
          }

          if (msg.role === 'agent-run') {
            const isHistorical = Array.isArray(msg.events) && msg.events.length > 0;
            return (
              <div key={idx} className="vsc-msg vsc-msg-agent">
                <AgentRunMessage
                  key={msg.runKey || idx}
                  task={msg.task}
                  model={msg.model}
                  initialEvents={isHistorical ? msg.events : undefined}
                  initialStatus={isHistorical ? (msg.finalStatus || 'done') : undefined}
                  onAgentDataChange={onAgentDataChange}
                  onComplete={(finalEv, allEvents) => {
                    setLoading(false);
                    // Persist the completed run so a refresh keeps it visible.
                    setMessages(prev => prev.map((m, i) =>
                      i === idx && m.role === 'agent-run'
                        ? { ...m, events: allEvents, finalStatus: finalEv?.status || 'done' }
                        : m
                    ));
                  }}
                />
              </div>
            );
          }

          if (msg.role === 'agent') {
            return (
              <div key={idx} className="vsc-msg vsc-msg-agent">
                <AgentMessage
                  messageId={msg.messageId}
                  onInsertCode={onInsertCode}
                  onOverwriteCode={onOverwriteCode}
                />
              </div>
            );
          }

          if (msg.role === 'assistant') {
            return (
              <div key={idx} className="vsc-msg vsc-msg-agent">
                {msg.error_type ? (
                  <div className="vsc-msg-error">
                    <AlertTriangle size={13} />
                    <span>{msg.error_message || msg.content}</span>
                  </div>
                ) : (
                  <div className="vsc-msg-content" style={{ whiteSpace: 'pre-wrap' }}>
                    {msg.content}
                  </div>
                )}
              </div>
            );
          }
          return null;
        })}
      </div>

      {/* Footer: model selector + mode toggle + input */}
      <div className="vsc-chat-footer">
        <div className="vsc-footer-row">
          <ModelSelector onModelChange={handleModelChange} refreshKey={providerRefreshKey} />
          <div className="vsc-mode-toggle" role="group" aria-label="Assistant mode" data-testid="agent-mode-toggle">
            <button
              type="button"
              className={`vsc-mode-btn ${!agentMode ? 'active' : ''}`}
              onClick={() => setAgentMode(false)}
              title="Ask mode — explanations & guided help"
              disabled={loading}
            >
              <MessageSquare size={12} /> Chat
            </button>
            <button
              type="button"
              className={`vsc-mode-btn ${agentMode ? 'active' : ''}`}
              onClick={() => setAgentMode(true)}
              title="Agent mode — autonomous build with tools"
              disabled={loading}
            >
              <Bot size={12} /> Agent
            </button>
          </div>
        </div>
        <div className={`vsc-input-area ${loading ? 'disabled' : ''}`}>
          {loading && (
            <div className="vsc-progress-bar">
              <div className="vsc-progress-fill" />
            </div>
          )}
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={loading ? "Generating..." : (agentMode ? "Describe what to build..." : "Ask a question...")}
            className="vsc-input"
            rows={1}
            disabled={loading}
            data-testid="chat-input"
          />
          <button
            onClick={handleSendMessage}
            disabled={!input.trim() || loading}
            className="vsc-send-btn"
            data-testid="send-message-button"
          >
            {loading ? <Square size={14} /> : <Send size={14} />}
          </button>
        </div>
      </div>
    </div>
    )
  );
};

const ChatAssistant = React.forwardRef(ChatAssistantComponent);
ChatAssistant.displayName = "ChatAssistant";

export default ChatAssistant;
