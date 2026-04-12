import React, { useState, useRef, useEffect, useCallback } from "react";
import axios from "axios";
import { useToast } from "./ToastProvider";
import { Send, User, Code, Copy, Check, RefreshCw, Sparkles, Eye, EyeOff, Zap, BookOpen, Calculator, AlertTriangle } from "lucide-react";
import { Button, Box, Chip } from '@mui/material';
import ModelSelector from "./ModelSelector";

const API = '/api';

const ChatAssistantComponent = ({ dslFunctions, events, onInsertCode, onOverwriteCode, editorCode, consoleOutput, providerRefreshKey }, ref) => {
  const toast = useToast();
  const [messages, setMessages] = useState(() => {
    try {
      const saved = localStorage.getItem('chatMessages');
      return saved ? JSON.parse(saved) : [];
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
  const [copiedIndex, setCopiedIndex] = useState(null);
  const [showContext, setShowContext] = useState(false);
  const [selectedModel, setSelectedModel] = useState("");
  const scrollRef = useRef(null);
  const inputRef = useRef(null);

  const handleModelChange = useCallback((model) => {
    setSelectedModel(model);
  }, []);

  // Expose sendMessage method to parent
  React.useImperativeHandle(ref, () => ({
    sendMessage: (message) => {
      if (message.trim()) {
        setMessages(prev => [...prev, { role: "user", content: message }]);
        handleSendMessageWithMessage(message);
      }
    }
  }));

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  // Persist messages and sessionId to localStorage when they change
  useEffect(() => {
    try {
      localStorage.setItem('chatMessages', JSON.stringify(messages || []));
      if (sessionId) localStorage.setItem('chatSessionId', sessionId);
      else localStorage.removeItem('chatSessionId');
    } catch (e) {
      // ignore
    }
  }, [messages, sessionId]);

  // Build context summary for display
  const getContextSummary = () => {
    const eventsCount = events?.length || 0;
    const functionsCount = dslFunctions?.length || 0;
    const hasEditorCode = editorCode && editorCode.trim().length > 0;
    const consoleLogsCount = consoleOutput?.length || 0;
    return { eventsCount, functionsCount, hasEditorCode, consoleLogsCount };
  };

  const handleSendMessageWithMessage = async (userMessage) => {
    setLoading(true);

    try {
      const context = {
        events: events || [],
        editor_code: editorCode || "",
        console_output: consoleOutput || [],
        dsl_functions: dslFunctions || [],
        helper_docs: {
          days_to_next: {
            description: "Calculate signed days from current row date to next row date (returns default when next is missing).",
            usage: "days_to_next(current_date, next_date, default=0)",
            note: "Accepts YYYY-MM-DD strings or datetime values; returns integer days."
          }
        },
        ai_requirements: "IMPORTANT: Follow these code-generation rules for ALL DSL examples: use ## for inline comments (never //), do NOT create transactions or call createTransaction/createTransactions unless the user explicitly asks for them, compute required values and use print() to output the final variable when transactions are NOT requested, use only DSL functions supported by both frontend and backend, never output Python or other languages, and ensure code is syntactically valid and runnable. Wrap code in ```dsl blocks when providing examples."
      };

        // Build conversation history for multi-turn context
        const history = messages.slice(-10).map(m => ({
          role: m.role,
          content: m.content,
        }));

        const response = await axios.post(`${API}/chat`, {
        message: userMessage,
        session_id: sessionId,
        context: context,
        model: selectedModel || undefined,
        history: history,
      });

      if (!sessionId) {
        setSessionId(response.data.session_id);
      }

      const data = response.data;
      const msg = {
        role: "assistant",
        content: data.response,
        structured: data.structured || null,
        error_type: data.error_type || null,
        error_message: data.error_message || null,
      };
      setMessages(prev => [...prev, msg]);
    } catch (error) {
      toast.error("Failed to get response from AI assistant");
      setMessages(prev => [...prev, { 
        role: "assistant", 
        content: "Sorry, I encountered an error. Please try again.",
        error_type: "network",
      }]);
    } finally {
      setLoading(false);
    }
  };

  const handleSendMessage = async () => {
    if (!input.trim()) return;

    const userMessage = input.trim();
    setInput("");
    
    await handleSendMessageWithMessage(userMessage);
  };

  // Extract ONLY code blocks from message content
  const extractCodeOnly = (content) => {
    const codeBlocks = [];
    const lines = content.split('\n');
    let inCodeBlock = false;
    let currentBlock = [];
    
    for (let line of lines) {
      if (line.trim().startsWith('```')) {
        if (inCodeBlock) {
          if (currentBlock.length > 0) {
            codeBlocks.push(currentBlock.join('\n'));
          }
          currentBlock = [];
        }
        inCodeBlock = !inCodeBlock;
        continue;
      }
      
      if (inCodeBlock) {
        currentBlock.push(line);
      } else {
        const trimmed = line.trim();
        const isAssignment = /^[a-z_][a-zA-Z0-9_]*\s*=\s*.+/.test(trimmed);
        const isFunctionCall = /^[a-z_][a-zA-Z0-9_]*\s*\(/.test(trimmed) && !trimmed.startsWith('//') && !trimmed.startsWith('##');
        const isCreateTransaction = trimmed.startsWith('createTransaction(');
        const isPrint = trimmed.startsWith('print(') || trimmed.startsWith('print_');
        
        if ((isAssignment || isFunctionCall || isCreateTransaction || isPrint) && !trimmed.startsWith('//') && !trimmed.startsWith('##')) {
          codeBlocks.push(trimmed);
        }
      }
    }
    
    if (currentBlock.length > 0) {
      codeBlocks.push(currentBlock.join('\n'));
    }
    
    return codeBlocks.join('\n\n');
  };

  const hasCode = (content) => {
    const code = extractCodeOnly(content);
    return code.trim().length > 0;
  };

  const handleCopyCode = (content, idx) => {
    const code = extractCodeOnly(content);
    navigator.clipboard.writeText(code);
    setCopiedIndex(idx);
    setTimeout(() => setCopiedIndex(null), 2000);
    toast.success("Code copied to clipboard");
  };

  const handleInsertCode = (content) => {
    const code = extractCodeOnly(content);
    if (code.trim()) {
      onInsertCode(code);
      toast.success("Code inserted into editor");
    }
  };

  const handleOverwriteCode = (content) => {
    const code = extractCodeOnly(content);
    if (code.trim() && onOverwriteCode) {
      onOverwriteCode(code);
      toast.success("Editor replaced with code");
    }
  };

  const handleClearChat = () => {
    setMessages([]);
    setSessionId(null);
    try {
      localStorage.removeItem('chatMessages');
      localStorage.removeItem('chatSessionId');
    } catch (e) {
      // ignore
    }
  };

  // Render message content with Copilot-style markdown and colors
  const MessageContent = ({ content, isUser }) => {
    // Parse markdown and return styled elements
    const parseMarkdown = (text, isUser) => {
      const parts = [];
      let lastIndex = 0;

      // Pattern for: **bold**, *italic*, `code`, [link](url), - list, numbers
      const patterns = [
        { regex: /\*\*(.*?)\*\*/g, type: 'bold' },
        { regex: /\*(.*?)\*/g, type: 'italic' },
        { regex: /`(.*?)`/g, type: 'inline-code' },
        { regex: /\[(.*?)\]\((.*?)\)/g, type: 'link' },
        { regex: /^- (.*?)$/gm, type: 'list-item' },
        { regex: /^\d+\. (.*?)$/gm, type: 'numbered-item' },
      ];

      // Split by patterns and reconstruct
      let processedText = text;
      
      // Handle bold
      processedText = processedText.replace(/\*\*(.*?)\*\*/g, '<BOLD>$1</BOLD>');
      // Handle italic  
      processedText = processedText.replace(/\*(.*?)\*/g, '<ITALIC>$1</ITALIC>');
      // Handle inline code
      processedText = processedText.replace(/`(.*?)`/g, '<CODE>$1</CODE>');

      const lines = processedText.split('\n');
      
      return lines.map((line, lineIdx) => {
        if (!line.trim()) {
          return <p key={lineIdx} className="my-0.5">&nbsp;</p>;
        }

        // Handle list items
        const listMatch = line.match(/^- (.*?)$/);
        if (listMatch) {
          return (
            <div key={lineIdx} className="flex gap-2 my-1">
              <span className={isUser ? 'text-white' : 'text-blue-500'}>•</span>
              <span className={isUser ? 'text-white' : 'text-slate-700'}>{renderLineContent(listMatch[1], isUser)}</span>
            </div>
          );
        }

        // Handle numbered items
        const numMatch = line.match(/^(\d+)\. (.*?)$/);
        if (numMatch) {
          return (
            <div key={lineIdx} className="flex gap-2 my-1">
              <span className={isUser ? 'text-white' : 'text-blue-500'}>{numMatch[1]}.</span>
              <span className={isUser ? 'text-white' : 'text-slate-700'}>{renderLineContent(numMatch[2], isUser)}</span>
            </div>
          );
        }

        return <p key={lineIdx} className={`my-1 ${isUser ? 'text-white' : 'text-slate-700'}`}>{renderLineContent(line, isUser)}</p>;
      });
    };

    const renderLineContent = (text, isUser) => {
      const parts = [];
      let lastIndex = 0;
      const regex = /<(BOLD|ITALIC|CODE)>(.*?)<\/\1>/g;
      let match;

      while ((match = regex.exec(text)) !== null) {
        if (match.index > lastIndex) {
          parts.push(text.substring(lastIndex, match.index));
        }

        const [, type, content] = match;
        if (type === 'BOLD') {
          parts.push(
            <strong key={parts.length} className={isUser ? 'font-semibold text-white' : 'font-semibold text-slate-900'}>
              {content}
            </strong>
          );
        } else if (type === 'ITALIC') {
          parts.push(
            <em key={parts.length} className={isUser ? 'text-white' : 'text-slate-700'}>
              {content}
            </em>
          );
        } else if (type === 'CODE') {
          parts.push(
            <code key={parts.length} className={`px-1.5 py-0.5 rounded text-xs font-mono ${
              isUser 
                ? 'bg-blue-500 text-white' 
                : 'bg-slate-200 text-slate-900'
            }`}>
              {content}
            </code>
          );
        }

        lastIndex = match.index + match[0].length;
      }

      if (lastIndex < text.length) {
        parts.push(text.substring(lastIndex));
      }

      return parts.length > 0 ? parts : text;
    };

    const parts = [];
    const lines = content.split('\n');
    let inCodeBlock = false;
    let codeBlockLines = [];
    let textLines = [];

    const flushText = () => {
      if (textLines.length > 0) {
        parts.push({ type: 'text', content: textLines.join('\n') });
        textLines = [];
      }
    };

    const flushCode = () => {
      if (codeBlockLines.length > 0) {
        parts.push({ type: 'code', content: codeBlockLines.join('\n') });
        codeBlockLines = [];
      }
    };

    for (let line of lines) {
      if (line.trim().startsWith('```')) {
        if (inCodeBlock) {
          flushCode();
        } else {
          flushText();
        }
        inCodeBlock = !inCodeBlock;
        continue;
      }

      if (inCodeBlock) {
        codeBlockLines.push(line);
      } else {
        textLines.push(line);
      }
    }

    flushText();
    flushCode();

    return (
      <div style={{ fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', Arial, sans-serif", fontSize: '13px', lineHeight: '1.6' }}>
        {parts.map((part, idx) => {
          if (part.type === 'code') {
            return (
              <div key={idx} className="my-3 rounded-lg overflow-hidden border border-slate-200 bg-slate-950">
                <pre className="p-3 overflow-x-auto">
                  <code className="text-[12px] font-mono text-slate-100 whitespace-pre-wrap leading-relaxed">
                    {part.content}
                  </code>
                </pre>
              </div>
            );
          }
          // For non-code description, replace leading comment markers (// or ##) with #-----
          let desc = part.content;
          if (typeof desc === 'string') {
            desc = desc.replace(/^\s*(\/\/|##)/, '#-----');
          }
          return (
            <div key={idx}>
              {parseMarkdown(desc, isUser)}
            </div>
          );
        })}
      </div>
    );
  };

  const handleKeyPress = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  const contextSummary = getContextSummary();

  return (
    <div className="w-[504px] bg-white border-l border-slate-200 flex flex-col h-full" style={{ fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif" }} data-testid="chat-assistant">
      {/* Header - Fyntrac style */}
      <div className="px-4 py-3 border-b border-[#E9ECEF] bg-white">
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2">
            <div className="w-6 h-6 rounded-lg bg-gradient-to-br from-[#5B5FED] to-[#4346C8] flex items-center justify-center flex-shrink-0 shadow-sm">
              <Sparkles className="w-3.5 h-3.5 text-white" />
            </div>
            <span className="text-sm font-semibold text-[#212529]">AI Assistant</span>
          </div>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Button 
              variant="text" 
              size="small"
              onClick={() => setShowContext(!showContext)}
              sx={{ minWidth: 'auto', px: 0.75, py: 0.5, color: '#6C757D', borderRadius: '6px', '&:hover': { bgcolor: '#F8F9FA' } }}
            >
              {showContext ? <EyeOff size={14} /> : <Eye size={14} />}
            </Button>
            {messages.length > 0 && (
              <Button 
                variant="text" 
                size="small"
                onClick={handleClearChat}
                sx={{ minWidth: 'auto', px: 0.75, py: 0.5, color: '#6C757D', borderRadius: '6px', '&:hover': { bgcolor: '#F8F9FA' } }}
              >
                <RefreshCw size={14} />
              </Button>
            )}
          </Box>
        </div>
        
        {/* Context Indicator - Fyntrac purple theme */}
        {showContext && (
          <div className="mt-2 p-2.5 bg-[#EEF0FE] rounded-lg border border-[#D4D6FA]">
            <p className="text-xs font-semibold text-[#5B5FED] mb-2">Context</p>
            <div className="flex flex-wrap gap-1.5 mb-1">
              <Chip label={`${contextSummary.eventsCount} Events`} size="small" sx={{ bgcolor: 'white', border: '1px solid #E9ECEF', color: '#495057', fontSize: '0.75rem', fontWeight: 500 }} />
              <Chip label={`${contextSummary.functionsCount} Functions`} size="small" sx={{ bgcolor: 'white', border: '1px solid #E9ECEF', color: '#495057', fontSize: '0.75rem', fontWeight: 500 }} />
              {contextSummary.hasEditorCode && (
                <Chip label="Editor" size="small" sx={{ bgcolor: '#D4EDDA', border: '1px solid #C3E6CB', color: '#155724', fontSize: '0.75rem', fontWeight: 500 }} />
              )}
              {contextSummary.consoleLogsCount > 0 && (
                <Chip label="Console" size="small" sx={{ bgcolor: '#FFF3CD', border: '1px solid #FFEEBA', color: '#856404', fontSize: '0.75rem', fontWeight: 500 }} />
              )}
            </div>
          </div>
        )}
      </div>

      {/* Messages - Copilot conversation style */}
      <Box sx={{ flex: 1, overflowY: 'auto' }} ref={scrollRef}>
        <div className="p-4 space-y-4">
          {messages.length === 0 && (
            <div className="text-center py-8">
              <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-[#EEF0FE] to-[#D4D6FA] flex items-center justify-center mx-auto mb-3 shadow-sm">
                <Sparkles className="w-6 h-6 text-[#5B5FED]" />
              </div>
              <h3 className="text-sm font-semibold text-[#212529] mb-1">How can I help?</h3>
              <p className="text-xs text-[#6C757D] max-w-[280px] mx-auto mb-5">
                Ask me anything about DSL, financial functions, or code generation. I can help with calculations, schedules, and transactions.
              </p>
              <div className="space-y-2.5">
                <button 
                  onClick={() => setInput("How do I create a simple interest calculation?")}
                  className="w-full text-left px-3 py-3 rounded-lg bg-[#EEF0FE] hover:bg-[#E0E2FD] border border-[#D4D6FA] text-xs transition-all duration-200 group"
                >
                  <div className="flex items-center gap-2.5">
                    <div className="w-7 h-7 rounded-md bg-[#5B5FED] flex items-center justify-center">
                      <Calculator className="w-3.5 h-3.5 text-white" />
                    </div>
                    <div className="text-left">
                      <div className="font-medium text-[#212529]">Calculate interest</div>
                      <div className="text-[#6C757D] text-xs">Using compound interest formula</div>
                    </div>
                  </div>
                </button>
                <button 
                  onClick={() => setInput("Show me how to create a loan amortization schedule")}
                  className="w-full text-left px-3 py-3 rounded-lg bg-[#D4EDDA] hover:bg-[#C3E6CB] border border-[#B8DAFF] text-xs transition-all duration-200 group"
                >
                  <div className="flex items-center gap-2.5">
                    <div className="w-7 h-7 rounded-md bg-[#28A745] flex items-center justify-center">
                      <Zap className="w-3.5 h-3.5 text-white" />
                    </div>
                    <div className="text-left">
                      <div className="font-medium text-[#212529]">Loan amortization</div>
                      <div className="text-[#6C757D] text-xs">Create payment schedules</div>
                    </div>
                  </div>
                </button>
                <button 
                  onClick={() => setInput("What functions are available for date calculations?")}
                  className="w-full text-left px-3 py-3 rounded-lg bg-[#FFF3CD] hover:bg-[#FFEEBA] border border-[#FFE8A1] text-xs transition-all duration-200 group"
                >
                  <div className="flex items-center gap-2.5">
                    <div className="w-7 h-7 rounded-md bg-[#FFC107] flex items-center justify-center">
                      <BookOpen className="w-3.5 h-3.5 text-white" />
                    </div>
                    <div className="text-left">
                      <div className="font-medium text-[#212529]">Date functions</div>
                      <div className="text-[#6C757D] text-xs">Work with dates and periods</div>
                    </div>
                  </div>
                </button>
              </div>
            </div>
          )}
          
          {messages.map((msg, idx) => (
            <div key={idx} className={`flex gap-3 ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
              {msg.role === 'assistant' && (
                <div className={`w-6 h-6 rounded-lg flex items-center justify-center flex-shrink-0 mt-1 shadow-sm ${
                  msg.error_type
                    ? 'bg-gradient-to-br from-amber-500 to-red-500'
                    : 'bg-gradient-to-br from-[#5B5FED] to-[#4346C8]'
                }`}>
                  {msg.error_type ? (
                    <AlertTriangle className="w-3.5 h-3.5 text-white" />
                  ) : (
                    <Sparkles className="w-3.5 h-3.5 text-white" />
                  )}
                </div>
              )}
              
              <div className={`max-w-[85%] ${msg.role === 'user' ? 'flex flex-col items-end' : ''}`}>
                <div className={`rounded-xl px-4 py-3 text-sm ${
                  msg.role === 'user' 
                    ? 'bg-gradient-to-br from-[#5B5FED] to-[#4346C8] text-white rounded-br-sm shadow-md' 
                    : msg.error_type
                      ? 'bg-amber-50 text-amber-900 border border-amber-200 rounded-bl-sm shadow-sm'
                      : 'bg-white text-[#495057] border border-[#E9ECEF] rounded-bl-sm shadow-sm hover:shadow-md transition-shadow'
                }`}>
                  <MessageContent content={msg.error_message || msg.content} isUser={msg.role === 'user'} />
                </div>
                
                {/* Action buttons - Fyntrac style */}
                {msg.role === 'assistant' && hasCode(msg.content) && (
                  <div className="flex gap-1.5 mt-2 ml-6">
                    <button 
                      className="text-xs text-[#6C757D] hover:text-[#5B5FED] hover:bg-[#EEF0FE] px-2 py-1.5 rounded-md transition-all duration-200 flex items-center gap-1.5 font-medium border border-transparent hover:border-[#D4D6FA]"
                      onClick={() => handleCopyCode(msg.content, idx)}
                      title="Copy code"
                    >
                      {copiedIndex === idx ? (
                        <><Check className="w-3.5 h-3.5 text-[#28A745]" /> Copied</>
                      ) : (
                        <><Copy className="w-3.5 h-3.5" /> Copy</>
                      )}
                    </button>
                    <button 
                      className="text-xs text-[#6C757D] hover:text-[#5B5FED] hover:bg-[#EEF0FE] px-2 py-1.5 rounded-md transition-all duration-200 flex items-center gap-1.5 font-medium border border-transparent hover:border-[#D4D6FA]"
                      onClick={() => handleInsertCode(msg.content)}
                      title="Insert into editor"
                    >
                      <Code className="w-3.5 h-3.5" /> Insert
                    </button>
                    {onOverwriteCode && (
                      <button 
                        className="text-xs text-[#6C757D] hover:text-[#5B5FED] hover:bg-[#EEF0FE] px-2 py-1.5 rounded-md transition-all duration-200 flex items-center gap-1.5 font-medium border border-transparent hover:border-[#D4D6FA]"
                        onClick={() => handleOverwriteCode(msg.content)}
                        title="Replace editor content"
                      >
                        <RefreshCw className="w-3.5 h-3.5" /> Replace
                      </button>
                    )}
                  </div>
                )}
              </div>
              
              {msg.role === 'user' && (
                <div className="w-6 h-6 rounded-lg bg-[#ADB5BD] flex items-center justify-center flex-shrink-0 mt-1 shadow-sm">
                  <User className="w-3.5 h-3.5 text-white" />
                </div>
              )}
            </div>
          ))}
          
          {loading && (
            <div className="flex gap-3">
              <div className="w-6 h-6 rounded-lg bg-gradient-to-br from-[#5B5FED] to-[#4346C8] flex items-center justify-center flex-shrink-0">
                <Sparkles className="w-3.5 h-3.5 text-white animate-pulse" />
              </div>
              <div className="bg-white border border-[#E9ECEF] rounded-xl px-4 py-3 rounded-bl-sm shadow-sm">
                <div className="flex gap-1.5">
                  <div className="w-2 h-2 bg-[#5B5FED] rounded-full animate-bounce" style={{ animationDelay: '0ms' }}></div>
                  <div className="w-2 h-2 bg-[#5B5FED] rounded-full animate-bounce" style={{ animationDelay: '150ms' }}></div>
                  <div className="w-2 h-2 bg-[#5B5FED] rounded-full animate-bounce" style={{ animationDelay: '300ms' }}></div>
                </div>
              </div>
            </div>
          )}
        </div>
      </Box>

      {/* Model selector */}
      <div className="px-3 pt-1.5 pb-0 border-t border-[#E9ECEF] bg-white">
        <ModelSelector onModelChange={handleModelChange} refreshKey={providerRefreshKey} />
      </div>

      {/* Input - Fyntrac style */}
      <div className="p-3 pt-1.5 border-[#E9ECEF] bg-white">
        <div className="flex items-end gap-2 rounded-lg border border-[#CED4DA] bg-white focus-within:border-[#5B5FED] focus-within:ring-2 focus-within:ring-[#5B5FED] focus-within:ring-opacity-20 transition-all duration-200">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="Ask anything..."
            className="flex-1 bg-transparent text-sm text-[#212529] placeholder-[#ADB5BD] resize-none border-0 focus:outline-none focus:ring-0 p-3 min-h-[40px] max-h-[120px]"
            style={{ fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', sans-serif" }}
            rows={1}
            disabled={loading}
            data-testid="chat-input"
          />
          <Button 
            onClick={handleSendMessage} 
            disabled={!input.trim() || loading}
            size="small"
            variant="contained"
            data-testid="send-message-button"
            sx={{
              m: 1,
              minWidth: 'auto',
              height: '32px',
              width: '32px',
              p: 0,
              borderRadius: '8px',
              bgcolor: '#14213D',
              '&:hover': { bgcolor: '#1D3557' },
              '&:disabled': { bgcolor: '#CED4DA', color: '#6C757D' }
            }}
          >
            <Send className="w-4 h-4" />
          </Button>
        </div>
      </div>
    </div>
  );
};
const ChatAssistant = React.forwardRef(ChatAssistantComponent);
ChatAssistant.displayName = "ChatAssistant";

export default ChatAssistant;
