import React, { useState, useRef, useEffect, useCallback } from "react";
import { useToast } from "./ToastProvider";
import { Send, RotateCcw, AlertTriangle, Square, Sparkles } from "lucide-react";
import ModelSelector from "./ModelSelector";
import AgentMessage from "./agent/AgentMessage";
import { runAgentPipeline, generateMessageId } from "../agent/agentPipeline";
import { detectFunctionMention, getExplanation, formatForChat } from "../agent/testing/explanationStore";
import "./ChatAssistant.css";

const ChatAssistantComponent = ({ dslFunctions, events, onInsertCode, onOverwriteCode, editorCode, consoleOutput, editorRef, monacoRef, providerRefreshKey }, ref) => {
  const toast = useToast();

  const [messages, setMessages] = useState(() => {
    try {
      const saved = localStorage.getItem('chatMessages');
      if (saved) {
        const parsed = JSON.parse(saved);
        return parsed.filter(m => m.role === 'user' || (m.role === 'assistant' && m.content));
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
  const scrollRef = useRef(null);
  const inputRef = useRef(null);

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
      const persistable = messages.filter(m => m.role === 'user' || (m.role === 'assistant' && m.content));
      localStorage.setItem('chatMessages', JSON.stringify(persistable));
      if (sessionId) localStorage.setItem('chatSessionId', sessionId);
      else localStorage.removeItem('chatSessionId');
    } catch (e) { /* ignore */ }
  }, [messages, sessionId]);

  const handleSendWithMessage = async (userMessage) => {
    setLoading(true);

    // Check if the user is asking about a known DSL function.
    // If we have a pre-built explanation, inject it as an instant response
    // before the AI pipeline (which provides richer, contextual answers).
    const functionName = detectFunctionMention(userMessage);
    const explanation = functionName ? getExplanation(functionName) : null;
    if (explanation) {
      const formatted = formatForChat(explanation);
      setMessages(prev => [...prev, { role: 'assistant', content: formatted }]);
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
    <div className="vsc-chat" data-testid="chat-assistant">
      {/* Header - Fyntrac style */}
      <div className="vsc-chat-header">
        <div className="vsc-header-left">
          <div className="vsc-header-logo">
            <Sparkles className="vsc-header-logo-icon" />
          </div>
          <span className="vsc-chat-title">AI Assistant</span>
        </div>
        {messages.filter(m => !m._hidden).length > 0 && (
          <button className="vsc-icon-btn" onClick={handleClearChat} title="New conversation" disabled={loading}>
            <RotateCcw size={14} />
          </button>
        )}
      </div>

      {/* Messages */}
      <div className="vsc-chat-messages" ref={scrollRef}>
        {/* Empty state */}
        {messages.filter(m => !m._hidden).length === 0 && (
          <div className="vsc-empty-state">
            <p className="vsc-empty-title">How can I help with your calculations?</p>
            <p className="vsc-empty-subtitle">
              Ask me to write formulas, troubleshoot errors, explain calculations, or build financial rules.
            </p>
            <div className="vsc-suggestions">
              {[
                "How do I create a simple interest calculation?",
                "Show me a loan amortization schedule",
                "What date formulas are available?",
              ].map((q, i) => (
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

      {/* Footer: model selector + input */}
      <div className="vsc-chat-footer">
        <ModelSelector onModelChange={handleModelChange} refreshKey={providerRefreshKey} />
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
            placeholder={loading ? "Generating..." : "Ask a question..."}
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
  );
};

const ChatAssistant = React.forwardRef(ChatAssistantComponent);
ChatAssistant.displayName = "ChatAssistant";

export default ChatAssistant;
