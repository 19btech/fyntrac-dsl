import React, { useState, useRef, useEffect, useCallback } from "react";
import { useToast } from "./ToastProvider";
import {
  Send, Square, Sparkles, Bot, MessageSquare, ChevronLeft, ChevronRight,
  Paperclip, History, Plus, Trash2, FileSpreadsheet, ArrowDown, Copy, Check,
  FileText,
} from "lucide-react";
import {
  Box, Paper, Stack, Typography, IconButton, Tooltip, TextField, Chip,
  Avatar, Drawer, List, ListItemButton, ListItemText, Divider,
  ToggleButton, ToggleButtonGroup, Badge, InputAdornment,
  Menu, MenuItem, ListItemIcon,
} from "@mui/material";
import ModelSelector from "./ModelSelector";
import AgentMessage from "./agent/AgentMessage";
import AgentRunMessage from "./agent/AgentRunMessage";
import MarkdownLite from "./agent/MarkdownLite";
import { runAgentPipeline, generateMessageId } from "../agent/agentPipeline";
import { detectFunctionMention, getExplanation, formatForChat, detectConceptMention, getConcept, formatConceptForChat } from "../agent/testing/explanationStore";
import "./ChatAssistant.css";

// ── Multi-conversation store (localStorage) ────────────────────────────────
// Each chat: { id, title, updatedAt, sessionId, messages, workbooks }
// `workbooks` records the Excel files uploaded during that conversation so
// reopening a chat shows which model files the agent had available.
const CHATS_KEY = "fyntracChats";
const CURRENT_CHAT_KEY = "fyntracCurrentChatId";
const MAX_CHATS = 20;

const genChatId = () =>
  `c_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

const loadChats = () => {
  try {
    const raw = localStorage.getItem(CHATS_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch (e) {
    return [];
  }
};

const saveChats = (chats) => {
  try {
    localStorage.setItem(CHATS_KEY, JSON.stringify(chats.slice(0, MAX_CHATS)));
  } catch (e) { /* ignore quota */ }
};

const persistableMessages = (messages) => messages.filter(m =>
  m.role === "user"
  || (m.role === "assistant" && m.content)
  || (m.role === "agent-run" && m.task)
);

// Any agent run that came from persistence (page refresh or loading a prior
// conversation) is HISTORICAL — it must replay its saved timeline, never
// re-execute. Fresh runs created by clicking Send never carry this flag.
const markReplay = (messages) => messages.map(m =>
  m.role === "agent-run" ? { ...m, _replay: true } : m
);

const chatTitle = (messages) => {
  const first = messages.find(m => m.role === "user" || (m.role === "agent-run" && m.task));
  const text = first ? (first.content || first.task || "") : "";
  const clean = text.replace(/\s+/g, " ").trim();
  return clean ? (clean.length > 60 ? clean.slice(0, 57) + "…" : clean) : "New conversation";
};

const fmtWhen = (ts) => {
  if (!ts) return "";
  const d = new Date(ts);
  const today = new Date();
  const sameDay = d.toDateString() === today.toDateString();
  return sameDay
    ? d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    : d.toLocaleDateString([], { month: "short", day: "numeric" });
};

// Day bucket key + human label for sticky date separators between turns.
const dayKey = (ts) => (ts ? new Date(ts).toDateString() : "");
const dayLabel = (ts) => {
  if (!ts) return "";
  const d = new Date(ts);
  const today = new Date();
  const yst = new Date(); yst.setDate(today.getDate() - 1);
  if (d.toDateString() === today.toDateString()) return "Today";
  if (d.toDateString() === yst.toDateString()) return "Yesterday";
  const sameYear = d.getFullYear() === today.getFullYear();
  return d.toLocaleDateString([], sameYear
    ? { weekday: "short", month: "short", day: "numeric" }
    : { month: "short", day: "numeric", year: "numeric" });
};

const ChatAssistantComponent = ({ dslFunctions, events, onInsertCode, onOverwriteCode, editorCode, consoleOutput, editorRef, monacoRef, providerRefreshKey, uiContext, onAgentDataChange, collapsed = false, onToggleCollapsed }, ref) => {
  const toast = useToast();

  const [currentChatId, setCurrentChatId] = useState(() => {
    try {
      return localStorage.getItem(CURRENT_CHAT_KEY) || genChatId();
    } catch (e) {
      return genChatId();
    }
  });
  const [messages, setMessages] = useState(() => {
    try {
      const saved = localStorage.getItem("chatMessages");
      return saved ? markReplay(persistableMessages(JSON.parse(saved))) : [];
    } catch (e) {
      return [];
    }
  });
  const [workbooks, setWorkbooks] = useState(() => {
    const chat = loadChats().find(c => c.id === (localStorage.getItem(CURRENT_CHAT_KEY) || ""));
    return (chat && chat.workbooks) || [];
  });
  const [documents, setDocuments] = useState(() => {
    const chat = loadChats().find(c => c.id === (localStorage.getItem(CURRENT_CHAT_KEY) || ""));
    return (chat && chat.documents) || [];
  });
  const [chats, setChats] = useState(loadChats);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  // When an agent run is active, AgentRunMessage publishes its stop handler
  // here so the chat input's send button can double as a Stop button.
  const [stopHandler, setStopHandler] = useState(null);
  const [sessionId, setSessionId] = useState(() => {
    try {
      return localStorage.getItem("chatSessionId") || null;
    } catch (e) {
      return null;
    }
  });
  const [selectedModel, setSelectedModel] = useState("");
  const [agentMode, setAgentMode] = useState(() => {
    try { return localStorage.getItem("chatAgentMode") === "1"; } catch (e) { return false; }
  });
  const [uploadingWorkbook, setUploadingWorkbook] = useState(false);
  const [uploadingDocument, setUploadingDocument] = useState(false);
  // Anchor element for the paperclip "attach" menu (Excel vs Requirements doc).
  const [attachAnchor, setAttachAnchor] = useState(null);
  const [showScrollBtn, setShowScrollBtn] = useState(false);
  const [copiedId, setCopiedId] = useState(null);
  const scrollRef = useRef(null);
  const inputRef = useRef(null);
  const fileInputRef = useRef(null);
  const docInputRef = useRef(null);
  const atBottomRef = useRef(true);

  // Track whether the user is near the bottom so streaming updates don't yank
  // them down while they read scrollback, and to toggle the jump-to-latest btn.
  const handleScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const dist = el.scrollHeight - el.scrollTop - el.clientHeight;
    atBottomRef.current = dist < 80;
    setShowScrollBtn(dist > 200);
  };
  const scrollToBottom = (behavior = "smooth") => {
    const el = scrollRef.current;
    if (el) el.scrollTo({ top: el.scrollHeight, behavior });
    atBottomRef.current = true;
    setShowScrollBtn(false);
  };
  const copyMessage = (id, text) => {
    try {
      navigator.clipboard.writeText(text || "");
      setCopiedId(id);
      setTimeout(() => setCopiedId(c => (c === id ? null : c)), 1500);
    } catch { /* ignore */ }
  };

  useEffect(() => {
    try { localStorage.setItem("chatAgentMode", agentMode ? "1" : "0"); } catch (e) { /* ignore */ }
  }, [agentMode]);

  useEffect(() => {
    try { localStorage.setItem(CURRENT_CHAT_KEY, currentChatId); } catch (e) { /* ignore */ }
  }, [currentChatId]);

  const handleModelChange = useCallback((model) => {
    setSelectedModel(model);
  }, []);

  // ── Persist the current conversation (legacy keys + history store) ──────
  useEffect(() => {
    const persistable = persistableMessages(messages);
    try {
      localStorage.setItem("chatMessages", JSON.stringify(persistable));
      if (sessionId) localStorage.setItem("chatSessionId", sessionId);
      else localStorage.removeItem("chatSessionId");
    } catch (e) { /* ignore */ }
    // Upsert into the multi-chat store only once there is real content.
    if (!persistable.length && !workbooks.length && !documents.length) return;
    const all = loadChats();
    const entry = {
      id: currentChatId,
      title: chatTitle(persistable),
      updatedAt: Date.now(),
      sessionId: sessionId || null,
      messages: persistable,
      workbooks,
      documents,
    };
    const idx = all.findIndex(c => c.id === currentChatId);
    if (idx >= 0) all[idx] = entry; else all.unshift(entry);
    all.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
    saveChats(all);
    setChats(all);
  }, [messages, sessionId, workbooks, documents, currentChatId]);

  const resetBackendSession = (sid) => {
    if (!sid) return;
    try {
      fetch(`/api/agent/sessions/${encodeURIComponent(sid)}/reset`, { method: "POST" }).catch(() => {});
    } catch (e) { /* ignore */ }
  };

  // Start a fresh conversation. The current one stays in history untouched.
  const handleNewChat = () => {
    if (loading) return;
    setMessages([]);
    setWorkbooks([]);
    setDocuments([]);
    setSessionId(null);
    setCurrentChatId(genChatId());
    setHistoryOpen(false);
    try {
      localStorage.removeItem("chatMessages");
      localStorage.removeItem("chatSessionId");
    } catch (e) { /* ignore */ }
  };

  const handleLoadChat = (chat) => {
    if (loading) return;
    // Loaded conversations are historical — replay only, never re-execute.
    setMessages(markReplay(persistableMessages(chat.messages || [])));
    setWorkbooks(chat.workbooks || []);
    setDocuments(chat.documents || []);
    setSessionId(chat.sessionId || null);
    setCurrentChatId(chat.id);
    setHistoryOpen(false);
  };

  const handleDeleteChat = (e, chatId) => {
    e.stopPropagation();
    const remaining = loadChats().filter(c => c.id !== chatId);
    saveChats(remaining);
    setChats(remaining);
    if (chatId === currentChatId) handleNewChat();
  };

  // ── Excel workbook upload (agent model-import workflow) ────────────────
  // .xlsx only — enforced here, on the <input accept>, and server-side.
  const handleWorkbookFile = async (e) => {
    const file = e.target.files && e.target.files[0];
    if (fileInputRef.current) fileInputRef.current.value = "";
    if (!file) return;
    if (!file.name.toLowerCase().endsWith(".xlsx")) {
      toast.error("Only Excel .xlsx workbooks can be uploaded");
      return;
    }
    setUploadingWorkbook(true);
    try {
      const form = new FormData();
      form.append("file", file);
      const res = await fetch("/api/agent/workbooks/upload", { method: "POST", body: form });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(body.detail || `Upload failed (${res.status})`);
      }
      toast.success(body.duplicate_of_existing
        ? "Workbook already uploaded — reusing it"
        : `Workbook '${body.filename}' uploaded`);
      setWorkbooks(prev => {
        if (prev.some(w => w.workbook_id === body.workbook_id)) return prev;
        return [...prev, {
          workbook_id: body.workbook_id,
          filename: body.filename,
          sheets: body.sheets || [],
          uploaded_at: body.uploaded_at || new Date().toISOString(),
        }];
      });
      setMessages(prev => [...prev, {
        role: "assistant",
        content: body.message || `Workbook '${body.filename}' uploaded (${(body.sheets || []).length} sheets).`,
        ts: Date.now(),
      }]);
      setAgentMode(true);
      setInput(`Analyse the uploaded workbook "${body.filename}" and rebuild it as DSL rules: ask me which sheets are inputs/calculations/outputs, reuse my existing events and data where they fit, translate the formulas, and verify the results.`);
      if (inputRef.current) inputRef.current.focus();
    } catch (err) {
      toast.error(err.message || "Workbook upload failed");
    } finally {
      setUploadingWorkbook(false);
    }
  };

  // ── Requirements document upload (PDF / Word) ──────────────────────────
  // The agent reads the doc's text, analyses it, asks clarifying questions,
  // then builds. .pdf / .docx only — enforced here, on <input accept>, and
  // server-side.
  const handleDocumentFile = async (e) => {
    const file = e.target.files && e.target.files[0];
    if (docInputRef.current) docInputRef.current.value = "";
    if (!file) return;
    const lower = file.name.toLowerCase();
    if (!lower.endsWith(".pdf") && !lower.endsWith(".docx")) {
      toast.error("Only PDF or Word (.docx) documents can be uploaded");
      return;
    }
    setUploadingDocument(true);
    try {
      const form = new FormData();
      form.append("file", file);
      const res = await fetch("/api/agent/documents/upload", { method: "POST", body: form });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(body.detail || `Upload failed (${res.status})`);
      }
      toast.success(body.duplicate_of_existing
        ? "Document already uploaded — reusing it"
        : `Document '${body.filename}' uploaded`);
      setDocuments(prev => {
        if (prev.some(d => d.document_id === body.document_id)) return prev;
        return [...prev, {
          document_id: body.document_id,
          filename: body.filename,
          kind: body.kind,
          pages: body.pages,
          paragraphs: body.paragraphs,
          uploaded_at: body.uploaded_at || new Date().toISOString(),
        }];
      });
      setMessages(prev => [...prev, {
        role: "assistant",
        content: body.message || `Requirements document '${body.filename}' uploaded.`,
        ts: Date.now(),
      }]);
      setAgentMode(true);
      setInput(`Read the uploaded requirements document "${body.filename}", summarise what you understand, and ask me any clarifying questions before building. Reuse my existing events and data where they fit.`);
      if (inputRef.current) inputRef.current.focus();
    } catch (err) {
      toast.error(err.message || "Document upload failed");
    } finally {
      setUploadingDocument(false);
    }
  };

  React.useImperativeHandle(ref, () => ({
    clearChat: () => {
      // Tell backend to drop the agent's memory for this session, then start
      // a fresh conversation (the old one stays available in History).
      resetBackendSession(sessionId);
      handleNewChat();
    },
    sendMessage: (message) => {
      if (message.trim()) {
        setMessages(prev => [...prev, { role: "user", content: message, ts: Date.now() }]);
        handleSendWithMessage(message);
      }
    },
    // Variant that forces agent mode for the next send (used by quick-action
    // buttons such as the Event Data viewer's "Generate Sample" button).
    sendAgentMessage: (message) => {
      if (!message.trim()) return;
      setAgentMode(true);
      setMessages(prev => [...prev, { role: "user", content: message, ts: Date.now() }]);
      handleSendWithMessage(message, { forceAgent: true });
    },
    // Silent variant used by the Ask AI button: no user bubble is shown.
    // funcName is the display name (e.g. "rate"); message is the full prompt.
    sendSilentMessage: (funcName, message) => {
      if (!message.trim()) return;
      const messageId = generateMessageId();
      setLoading(true);
      setMessages(prev => [...prev, { role: "agent", messageId }]);
      const heading = `**How does ${funcName}() function work in Fyntrac DSL?**\n\n`;
      runAgentPipeline(message, {
        messageId,
        events: events || [],
        editorCode: editorCode || "",
        consoleOutput: consoleOutput || [],
        dslFunctions: dslFunctions || [],
        editorRef,
        monacoRef,
        selectedModel: selectedModel || undefined,
        sessionId,
        uiContext: uiContext || null,
        history: messages
          .filter(m => m.role === "user" || (m.role === "assistant" && m.content))
          .slice(-10)
          .map(m => ({ role: m.role === "assistant" ? "assistant" : "user", content: m.content })),
      }).then(result => {
        if (result.fullText) {
          setMessages(prev => [
            ...prev,
            { role: "assistant", content: heading + result.fullText, _hidden: true },
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

  // Auto-scroll on new messages / while streaming — but ONLY if the user is
  // already near the bottom, so reading scrollback isn't interrupted.
  useEffect(() => {
    if (atBottomRef.current && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  useEffect(() => {
    if (!loading) return;
    const interval = setInterval(() => {
      if (atBottomRef.current && scrollRef.current) {
        scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
      }
    }, 150);
    return () => clearInterval(interval);
  }, [loading]);

  const handleSendWithMessage = async (userMessage, opts = {}) => {
    setLoading(true);

    // Agent mode: spawn an autonomous run instead of the explanation pipeline.
    if (agentMode || opts.forceAgent) {
      const runKey = generateMessageId();
      // Ensure a stable session_id exists so the agent runtime can persist
      // conversation history across runs in the same chat.
      let sid = sessionId;
      if (!sid) {
        try {
          sid = (window.crypto && window.crypto.randomUUID)
            ? window.crypto.randomUUID()
            : `s_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
        } catch (e) {
          sid = `s_${Date.now()}`;
        }
        setSessionId(sid);
        try { localStorage.setItem("chatSessionId", sid); } catch (e) { /* ignore */ }
      }
      setMessages(prev => [...prev, { role: "agent-run", runKey, task: userMessage, model: selectedModel || undefined, ts: Date.now() }]);
      return;
    }

    // Check if the user is asking about a known DSL function.
    const functionName = detectFunctionMention(userMessage);
    const explanation = functionName ? getExplanation(functionName) : null;
    if (explanation) {
      setMessages(prev => [...prev, { role: "assistant", content: formatForChat(explanation), ts: Date.now() }]);
    }

    // Same idea for UI concepts (Rule Builder, Saved Rules, Live Preview, etc.).
    const conceptKey = detectConceptMention(userMessage);
    const concept = conceptKey ? getConcept(conceptKey) : null;
    if (concept) {
      setMessages(prev => [...prev, { role: "assistant", content: formatConceptForChat(concept), ts: Date.now() }]);
    }

    const messageId = generateMessageId();
    setMessages(prev => [...prev, { role: "agent", messageId }]);

    try {
      const result = await runAgentPipeline(userMessage, {
        messageId,
        events: events || [],
        editorCode: editorCode || "",
        consoleOutput: consoleOutput || [],
        dslFunctions: dslFunctions || [],
        editorRef,
        monacoRef,
        selectedModel: selectedModel || undefined,
        sessionId,
        uiContext: uiContext || null,
        history: messages
          .filter(m => m.role === "user" || (m.role === "assistant" && m.content))
          .slice(-10)
          .map(m => ({ role: m.role === "assistant" ? "assistant" : "user", content: m.content })),
      });

      if (result.fullText) {
        setMessages(prev => [
          ...prev,
          { role: "assistant", content: result.fullText, _hidden: true },
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
    setMessages(prev => [...prev, { role: "user", content: userMessage, ts: Date.now() }]);
    scrollToBottom("auto");
    await handleSendWithMessage(userMessage);
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  const visibleMessages = messages.filter(m => !m._hidden);

  // ── Collapsed rail ─────────────────────────────────────────────────────
  if (collapsed) {
    return (
      <Paper
        elevation={0}
        data-testid="chat-assistant-collapsed"
        sx={{
          width: 44, height: "100%", display: "flex", flexDirection: "column",
          alignItems: "center", py: 1, gap: 1, borderRadius: 0,
          borderLeft: "1px solid", borderColor: "divider",
          transition: "width 200ms ease",
        }}
      >
        <Tooltip title="Expand AI Assistant" placement="left">
          <IconButton size="small" onClick={onToggleCollapsed} className="panel-toggle-btn"
            sx={{ border: "1px solid", borderColor: "divider", bgcolor: "background.paper", boxShadow: 1 }}>
            <ChevronLeft size={15} />
          </IconButton>
        </Tooltip>
        <Avatar sx={{ width: 28, height: 28, mt: 0.5, background: "linear-gradient(135deg, #6366f1, #8b5cf6)" }}>
          <Sparkles size={15} />
        </Avatar>
        {visibleMessages.length > 0 && (
          <Tooltip title={`${visibleMessages.length} message(s)`} placement="left">
            <Badge badgeContent={visibleMessages.length} color="primary" max={99}>
              <MessageSquare size={16} color="#6C757D" />
            </Badge>
          </Tooltip>
        )}
      </Paper>
    );
  }

  // ── Full panel ─────────────────────────────────────────────────────────
  return (
    <Paper
      elevation={0}
      data-testid="chat-assistant"
      sx={{
        width: 504, height: "100%", display: "flex", flexDirection: "column",
        borderRadius: 0, borderLeft: "1px solid", borderColor: "divider",
        transition: "width 200ms ease", position: "relative", overflow: "hidden",
      }}
    >
      {/* Header */}
      <Box sx={{
        px: 2, py: 1.25, display: "flex", alignItems: "center", gap: 1.25,
        borderBottom: "1px solid", borderColor: "divider", flexShrink: 0,
        bgcolor: "background.paper",
      }}>
        <Avatar sx={{ width: 34, height: 34, background: "linear-gradient(135deg, #6366f1, #8b5cf6)" }}>
          <Sparkles size={17} />
        </Avatar>
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 700, lineHeight: 1.2 }}>
            AI Assistant
          </Typography>
        </Box>
        <Tooltip title="Start a new conversation">
          <span>
            <IconButton size="small" onClick={handleNewChat} disabled={loading}
              data-testid="new-chat-button">
              <Plus size={17} />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Conversation history">
          <span>
            <IconButton size="small" onClick={() => setHistoryOpen(true)} disabled={loading}
              data-testid="chat-history-button">
              <Badge badgeContent={chats.length} color="primary" max={99}
                sx={{ "& .MuiBadge-badge": { fontSize: 9, height: 14, minWidth: 14 } }}>
                <History size={17} />
              </Badge>
            </IconButton>
          </span>
        </Tooltip>
        {onToggleCollapsed && (
          <Tooltip title="Collapse panel">
            <IconButton size="small" onClick={onToggleCollapsed} className="panel-toggle-btn"
              sx={{ border: "1px solid", borderColor: "divider", boxShadow: 1 }}>
              <ChevronRight size={15} />
            </IconButton>
          </Tooltip>
        )}
      </Box>

      {/* Files (Excel models + requirement docs) attached to this conversation */}
      {(workbooks.length > 0 || documents.length > 0) && (
        <Box sx={{
          px: 2, py: 0.75, display: "flex", flexWrap: "wrap", gap: 0.5,
          borderBottom: "1px solid", borderColor: "divider", bgcolor: "#fafbff", flexShrink: 0,
        }}>
          {workbooks.map(w => (
            <Tooltip key={w.workbook_id}
              title={`Excel model — ${(w.sheets || []).length} sheet(s), available to the agent as workbook_id ${w.workbook_id}`}>
              <Chip
                size="small"
                icon={<FileSpreadsheet size={13} />}
                label={w.filename}
                variant="outlined"
                color="success"
                sx={{ maxWidth: 220, "& .MuiChip-label": { fontSize: 11 } }}
              />
            </Tooltip>
          ))}
          {documents.map(d => (
            <Tooltip key={d.document_id}
              title={`Requirements ${d.kind === "pdf" ? "PDF" : "Word"} document — available to the agent as document_id ${d.document_id}`}>
              <Chip
                size="small"
                icon={<FileText size={13} />}
                label={d.filename}
                variant="outlined"
                color="warning"
                sx={{ maxWidth: 220, "& .MuiChip-label": { fontSize: 11 } }}
              />
            </Tooltip>
          ))}
        </Box>
      )}

      {/* Messages */}
      <Box ref={scrollRef} onScroll={handleScroll}
        role="log" aria-live="polite" aria-relevant="additions text"
        aria-label="Conversation with the AI assistant"
        className="chat-scroll"
        sx={{ flex: 1, overflowY: "auto", px: 2, py: 1.5, bgcolor: "#f7f8fa", position: "relative" }}>
        {visibleMessages.length === 0 && (
          <Stack alignItems="center" justifyContent="center" spacing={1.5}
            sx={{ height: "100%", textAlign: "center", px: 2 }}>
            <Avatar sx={{ width: 52, height: 52, background: "linear-gradient(135deg, #6366f1, #8b5cf6)" }}>
              {agentMode ? <Bot size={26} /> : <Sparkles size={26} />}
            </Avatar>
            <Typography variant="subtitle1" sx={{ fontWeight: 700 }}>
              {agentMode ? "What should the agent build?" : "How can I help with your calculations?"}
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ maxWidth: 360 }}>
              {agentMode
                ? "I define events, generate or import data, write rules with DSL functions only (no custom code), then dry-run and self-debug. Attach a file with the paperclip below — an Excel model I'll translate into rules, or a PDF/Word requirements document I'll read and build from. Destructive actions always need your approval."
                : "I explain DSL functions with worked examples and walk you through the Rule Builder step by step. Switch to Agent mode for autonomous builds, or use the AI Rule Generator inside the Rule Builder for full code generation."}
            </Typography>
            <Stack spacing={0.75} sx={{ width: "100%", maxWidth: 380, pt: 0.5 }}>
              {(agentMode ? [
                "Build IFRS9 ECL stage 1/2/3 with sample data for 5 loans",
                "Create an amortization rule for fixed-rate loans and verify totals",
                "Translate my uploaded Excel model into rules and reconcile it",
              ] : [
                "What does pmt() do? Show me with sample numbers.",
                "Walk me through building a loan amortization rule",
                "How do I add a Schedule step in the Rule Builder?",
              ]).map((q, i) => (
                <Chip
                  key={i}
                  label={q}
                  variant="outlined"
                  onClick={() => setInput(q)}
                  sx={{
                    justifyContent: "flex-start", height: "auto", py: 0.75,
                    borderRadius: 2, "& .MuiChip-label": { whiteSpace: "normal", fontSize: 12 },
                  }}
                />
              ))}
            </Stack>
          </Stack>
        )}

        <Stack spacing={0.75}>
          {visibleMessages.map((msg, idx) => {
            const prev = idx > 0 ? visibleMessages[idx - 1] : null;
            // Consecutive messages from the same sender on the same day are
            // "grouped": the assistant avatar shows only on the first of a run.
            const grouped = !!prev && prev.role === msg.role
              && dayKey(prev.ts) === dayKey(msg.ts);
            // Sticky date separator when the calendar day changes (only when we
            // have timestamps to compare).
            const showDay = !!msg.ts && (idx === 0 || dayKey(prev?.ts) !== dayKey(msg.ts));
            const dateSep = showDay ? (
              <Box key={`day${idx}`} className="chat-day-sep">
                <span>{dayLabel(msg.ts)}</span>
              </Box>
            ) : null;
            // Faint divider before a new user turn (skipped when a date
            // separator already breaks the flow, and never grouped).
            const turnDivider = (!dateSep && !grouped && msg.role === "user" && idx > 0) ? (
              <Divider key={`d${idx}`} sx={{ my: 0.5, opacity: 0.45 }} />
            ) : null;

            if (msg.role === "user") {
              return (
                <React.Fragment key={idx}>
                  {dateSep}
                  {turnDivider}
                  <Box
                    className="chat-msg-in"
                    sx={{
                      display: "flex", flexDirection: "column", alignItems: "flex-end",
                      mt: grouped ? -0.25 : 0,
                      "&:hover .msg-copy": { opacity: 1 },
                    }}
                  >
                    <Box sx={{
                      maxWidth: "85%", px: 1.5, py: 1,
                      bgcolor: "primary.main", color: "primary.contrastText",
                      borderRadius: "14px 14px 4px 14px",
                      fontSize: 13, lineHeight: 1.5, whiteSpace: "pre-wrap",
                      wordBreak: "break-word",
                    }}>
                      {msg.content}
                    </Box>
                    <Stack direction="row" alignItems="center" spacing={0.5} sx={{ mt: 0.25, pr: 0.5 }}>
                      <IconButton
                        className="msg-copy" size="small"
                        onClick={() => copyMessage(idx, msg.content)}
                        sx={{ opacity: 0, transition: "opacity 0.15s", p: 0.25 }}
                        title="Copy"
                      >
                        {copiedId === idx ? <Check size={12} /> : <Copy size={12} />}
                      </IconButton>
                      {msg.ts && (
                        <Typography variant="caption" color="text.secondary" sx={{ fontSize: 10 }}>
                          {fmtWhen(msg.ts)}
                        </Typography>
                      )}
                    </Stack>
                  </Box>
                </React.Fragment>
              );
            }

            if (msg.role === "agent-run") {
              // Replay (never re-execute) when the message came from
              // persistence (_replay) OR already carries a saved timeline.
              const isReplay = !!msg._replay
                || (Array.isArray(msg.events) && msg.events.length > 0);
              const runKey = msg.runKey;
              return (
                <React.Fragment key={runKey || idx}>
                  {dateSep}
                  <Box className="chat-msg-in" sx={{ width: "100%" }}>
                  <AgentRunMessage
                    key={runKey || idx}
                    task={msg.task}
                    model={msg.model}
                    sessionId={sessionId}
                    replay={isReplay}
                    initialEvents={isReplay ? (msg.events || []) : undefined}
                    initialStatus={isReplay ? (msg.finalStatus || "done") : undefined}
                    onAgentDataChange={onAgentDataChange}
                    onStopHandleReady={(fn) => setStopHandler(() => fn)}
                    onComplete={(finalEv, allEvents) => {
                      setLoading(false);
                      setStopHandler(null);
                      // Persist the completed run keyed by its stable runKey
                      // (NOT the visible index, which diverges from the full
                      // messages array when hidden messages are present).
                      setMessages(prev => prev.map(m =>
                        m.role === "agent-run" && m.runKey === runKey
                          ? { ...m, events: allEvents, finalStatus: finalEv?.status || "done" }
                          : m
                      ));
                    }}
                  />
                  </Box>
                </React.Fragment>
              );
            }

            if (msg.role === "agent") {
              return (
                <React.Fragment key={idx}>
                  {dateSep}
                  <Box className="chat-msg-in" sx={{ width: "100%" }}>
                    <AgentMessage
                      messageId={msg.messageId}
                      onInsertCode={onInsertCode}
                      onOverwriteCode={onOverwriteCode}
                    />
                  </Box>
                </React.Fragment>
              );
            }

            if (msg.role === "assistant") {
              return (
                <React.Fragment key={idx}>
                  {dateSep}
                  <Box className="chat-msg-in" sx={{ display: "flex", gap: 1, alignItems: "flex-start", mt: grouped ? -0.25 : 0, "&:hover .msg-copy": { opacity: 1 } }}>
                  {grouped ? (
                    <Box sx={{ width: 24, flexShrink: 0 }} />
                  ) : (
                    <Avatar sx={{ width: 24, height: 24, mt: 0.25, background: "linear-gradient(135deg, #6366f1, #8b5cf6)" }}>
                      <Sparkles size={12} />
                    </Avatar>
                  )}
                  <Box sx={{ maxWidth: "88%", minWidth: 0 }}>
                    {msg.error_type ? (
                      <Paper variant="outlined" sx={{
                        px: 1.5, py: 1, borderRadius: "4px 14px 14px 14px",
                        borderColor: "error.light", bgcolor: "#fff5f5",
                        display: "flex", gap: 0.75, alignItems: "center",
                      }}>
                        <Typography variant="body2" color="error.main" sx={{ fontSize: 13 }}>
                          {msg.error_message || msg.content}
                        </Typography>
                      </Paper>
                    ) : (
                      <Paper variant="outlined" sx={{
                        px: 1.5, py: 0.5, borderRadius: "4px 14px 14px 14px",
                        fontSize: 13, lineHeight: 1.55, wordBreak: "break-word",
                      }}>
                        <MarkdownLite text={msg.content} />
                      </Paper>
                    )}
                    <Stack direction="row" alignItems="center" spacing={0.5} sx={{ mt: 0.25, pl: 0.5 }}>
                      <IconButton
                        className="msg-copy" size="small"
                        onClick={() => copyMessage(idx, msg.error_message || msg.content)}
                        sx={{ opacity: 0, transition: "opacity 0.15s", p: 0.25 }}
                        title="Copy"
                      >
                        {copiedId === idx ? <Check size={12} /> : <Copy size={12} />}
                      </IconButton>
                      {msg.ts && (
                        <Typography variant="caption" color="text.secondary" sx={{ fontSize: 10 }}>
                          {fmtWhen(msg.ts)}
                        </Typography>
                      )}
                    </Stack>
                  </Box>
                  </Box>
                </React.Fragment>
              );
            }
            return null;
          })}

          {/* Live typing indicator while a plain-chat reply is generating.
              Agent runs render their own streaming timeline, so only show
              this when the last message is a user turn awaiting a reply. */}
          {loading && visibleMessages.length > 0
            && visibleMessages[visibleMessages.length - 1].role === "user" && (
            <Box className="chat-msg-in" sx={{ display: "flex", gap: 1, alignItems: "flex-start" }}>
              <Avatar sx={{ width: 24, height: 24, mt: 0.25, background: "linear-gradient(135deg, #6366f1, #8b5cf6)" }}>
                <Sparkles size={12} />
              </Avatar>
              <Paper variant="outlined" aria-label="Assistant is typing"
                sx={{ px: 1.5, py: 1, borderRadius: "4px 14px 14px 14px" }}>
                <Box className="chat-typing" aria-hidden="true">
                  <span /><span /><span />
                </Box>
              </Paper>
            </Box>
          )}
        </Stack>

        {/* Jump-to-latest button (only when scrolled up) */}
        {showScrollBtn && (
          <IconButton
            onClick={() => scrollToBottom()}
            size="small"
            aria-label="Jump to latest message"
            sx={{
              position: "sticky", bottom: 8, left: "100%", mr: 1,
              bgcolor: "background.paper", border: "1px solid", borderColor: "divider",
              boxShadow: 2, "&:hover": { bgcolor: "background.paper" },
            }}
            title="Jump to latest"
          >
            <ArrowDown size={16} />
          </IconButton>
        )}
      </Box>

      {/* Footer: model + mode + input */}
      <Box sx={{ px: 1.5, pt: 1, pb: 1.25, borderTop: "1px solid", borderColor: "divider", bgcolor: "background.paper", flexShrink: 0 }}>
        <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <ModelSelector onModelChange={handleModelChange} refreshKey={providerRefreshKey} />
          </Box>
          <ToggleButtonGroup
            size="small"
            exclusive
            value={agentMode ? "agent" : "chat"}
            onChange={(_, v) => { if (v) setAgentMode(v === "agent"); }}
            disabled={loading}
            data-testid="agent-mode-toggle"
            sx={{ "& .MuiToggleButton-root": { px: 1.25, py: 0.4, textTransform: "none", fontSize: 12, gap: 0.5 } }}
          >
            <ToggleButton value="chat">
              <Tooltip title="Ask mode — explanations & guided help">
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
                  <MessageSquare size={13} /> Chat
                </Box>
              </Tooltip>
            </ToggleButton>
            <ToggleButton value="agent">
              <Tooltip title="Agent mode — autonomous build with tools; can import Excel models">
                <Box sx={{ display: "flex", alignItems: "center", gap: 0.5 }}>
                  <Bot size={13} /> Agent
                </Box>
              </Tooltip>
            </ToggleButton>
          </ToggleButtonGroup>
        </Stack>

        <TextField
          inputRef={inputRef}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={loading
            ? "Generating…"
            : (agentMode
              ? "Describe what to build, or attach an Excel model…"
              : "Ask about DSL functions, rules, schedules…")}
          fullWidth
          multiline
          maxRows={5}
          size="small"
          disabled={loading}
          data-testid="chat-input"
          InputProps={{
            sx: {
              borderRadius: 3, fontSize: 13, alignItems: "flex-end", py: 0.75,
              transition: "box-shadow 0.15s, border-color 0.15s",
              "&.Mui-focused": { boxShadow: "0 0 0 3px rgba(99,102,241,0.15)" },
            },
            startAdornment: (
              <InputAdornment position="start" sx={{ alignSelf: "flex-end", mb: 0.25 }}>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                  onChange={handleWorkbookFile}
                  style={{ display: "none" }}
                  data-testid="workbook-file-input"
                />
                <input
                  ref={docInputRef}
                  type="file"
                  accept=".pdf,.docx,application/pdf,application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                  onChange={handleDocumentFile}
                  style={{ display: "none" }}
                  data-testid="document-file-input"
                />
                <Tooltip title={(uploadingWorkbook || uploadingDocument)
                  ? "Uploading…"
                  : "Attach a file — an Excel model or a requirements document"}>
                  <span>
                    <IconButton
                      size="small"
                      edge="start"
                      onClick={(e) => setAttachAnchor(e.currentTarget)}
                      disabled={loading || uploadingWorkbook || uploadingDocument}
                      data-testid="attach-button"
                      aria-label="Attach a file"
                    >
                      <Paperclip size={16} />
                    </IconButton>
                  </span>
                </Tooltip>
                <Menu
                  anchorEl={attachAnchor}
                  open={Boolean(attachAnchor)}
                  onClose={() => setAttachAnchor(null)}
                  anchorOrigin={{ vertical: "top", horizontal: "left" }}
                  transformOrigin={{ vertical: "bottom", horizontal: "left" }}
                  slotProps={{ paper: { sx: { mt: -1, borderRadius: 2, minWidth: 288, boxShadow: 6 } } }}
                >
                  <Typography variant="caption" sx={{ px: 2, pt: 1, pb: 0.5, display: "block", color: "text.secondary", fontWeight: 600, letterSpacing: 0.3 }}>
                    ATTACH A FILE
                  </Typography>
                  <MenuItem
                    onClick={() => { setAttachAnchor(null); fileInputRef.current && fileInputRef.current.click(); }}
                    sx={{ py: 1.25, alignItems: "flex-start" }}
                    data-testid="attach-excel"
                  >
                    <ListItemIcon sx={{ mt: 0.25 }}>
                      <FileSpreadsheet size={20} color="#1a7f4b" />
                    </ListItemIcon>
                    <ListItemText
                      primary="Excel calculation sheet"
                      secondary="A .xlsx model — the agent translates its formulas into rules and reconciles the results."
                      primaryTypographyProps={{ fontSize: 14, fontWeight: 600 }}
                      secondaryTypographyProps={{ fontSize: 12, sx: { whiteSpace: "normal" } }}
                    />
                  </MenuItem>
                  <MenuItem
                    onClick={() => { setAttachAnchor(null); docInputRef.current && docInputRef.current.click(); }}
                    sx={{ py: 1.25, alignItems: "flex-start" }}
                    data-testid="attach-document"
                  >
                    <ListItemIcon sx={{ mt: 0.25 }}>
                      <FileText size={20} color="#c2410c" />
                    </ListItemIcon>
                    <ListItemText
                      primary="Business requirements document"
                      secondary="A PDF or Word (.docx) spec — the agent reads it, confirms the details with you, then builds."
                      primaryTypographyProps={{ fontSize: 14, fontWeight: 600 }}
                      secondaryTypographyProps={{ fontSize: 12, sx: { whiteSpace: "normal" } }}
                    />
                  </MenuItem>
                </Menu>
              </InputAdornment>
            ),
            endAdornment: (
              <InputAdornment position="end" sx={{ alignSelf: "flex-end", mb: 0.25 }}>
                <Tooltip title={stopHandler ? "Stop agent" : "Send (Enter)"}>
                  <span>
                    {(() => {
                      const active = stopHandler || (input.trim() && !loading);
                      return (
                        <IconButton
                          size="small"
                          onClick={stopHandler
                            ? () => { try { stopHandler(); } catch (_) {} setStopHandler(null); }
                            : handleSendMessage}
                          disabled={stopHandler ? false : (!input.trim() || loading)}
                          data-testid={stopHandler ? "stop-agent-button" : "send-message-button"}
                          aria-label={stopHandler ? "Stop agent" : "Send message"}
                          sx={{
                            width: 32, height: 32,
                            background: active
                              ? "linear-gradient(135deg, #6366f1, #8b5cf6)"
                              : "transparent",
                            color: active ? "#fff" : "text.disabled",
                            boxShadow: active ? "0 2px 8px rgba(99,102,241,0.35)" : "none",
                            transition: "background 0.15s, box-shadow 0.15s, transform 0.1s",
                            "&:hover": { background: active
                              ? "linear-gradient(135deg, #4f46e5, #7c3aed)" : undefined,
                              transform: active ? "scale(1.06)" : undefined },
                            "&.Mui-disabled": { color: "text.disabled" },
                          }}
                        >
                          {stopHandler || loading ? <Square size={14} /> : <Send size={14} />}
                        </IconButton>
                      );
                    })()}
                  </span>
                </Tooltip>
              </InputAdornment>
            ),
          }}
        />
        <Typography variant="caption" color="text.secondary" sx={{ display: "block", mt: 0.5, px: 0.5, fontSize: 10.5 }}>
          {agentMode
            ? "The agent asks before anything destructive. Attach an Excel model or requirements document with the paperclip."
            : "Shift+Enter for a new line."}
        </Typography>
      </Box>

      {/* Conversation history drawer */}
      <Drawer
        anchor="right"
        open={historyOpen}
        onClose={() => setHistoryOpen(false)}
        PaperProps={{ sx: { width: 340 } }}
      >
        <Box sx={{ px: 2, py: 1.5, display: "flex", alignItems: "center", gap: 1 }}>
          <History size={17} />
          <Typography variant="subtitle2" sx={{ fontWeight: 700, flex: 1 }}>
            Conversations
          </Typography>
          <Tooltip title="Start a new conversation">
            <IconButton size="small" onClick={handleNewChat}>
              <Plus size={16} />
            </IconButton>
          </Tooltip>
        </Box>
        <Divider />
        {chats.length === 0 ? (
          <Box sx={{ p: 3, textAlign: "center" }}>
            <Typography variant="body2" color="text.secondary">
              No conversations yet. Your chats — including any Excel models or
              requirement documents you attach — are saved here automatically.
            </Typography>
          </Box>
        ) : (
          <List dense sx={{ overflowY: "auto" }}>
            {chats.map(chat => (
              <ListItemButton
                key={chat.id}
                selected={chat.id === currentChatId}
                onClick={() => handleLoadChat(chat)}
                sx={{ alignItems: "flex-start", py: 1 }}
              >
                <ListItemText
                  disableTypography
                  primary={
                    <Stack direction="row" alignItems="center" spacing={0.75}>
                      <Typography variant="body2" noWrap sx={{ fontWeight: 600, flex: 1 }}>
                        {chat.title || "New conversation"}
                      </Typography>
                      <Typography variant="caption" color="text.secondary" sx={{ flexShrink: 0 }}>
                        {fmtWhen(chat.updatedAt)}
                      </Typography>
                      <IconButton size="small" edge="end"
                        onClick={(e) => handleDeleteChat(e, chat.id)}
                        title="Delete conversation">
                        <Trash2 size={13} />
                      </IconButton>
                    </Stack>
                  }
                  secondary={
                    <Box>
                      <Typography variant="caption" color="text.secondary">
                        {(chat.messages || []).length} message(s)
                      </Typography>
                      {((chat.workbooks || []).length > 0 || (chat.documents || []).length > 0) && (
                        <Stack direction="row" spacing={0.5} sx={{ mt: 0.5, flexWrap: "wrap", gap: 0.5 }}>
                          {(chat.workbooks || []).map(w => (
                            <Chip
                              key={w.workbook_id}
                              size="small"
                              icon={<FileSpreadsheet size={11} />}
                              label={w.filename}
                              variant="outlined"
                              color="success"
                              sx={{ height: 20, maxWidth: 200, "& .MuiChip-label": { fontSize: 10 } }}
                            />
                          ))}
                          {(chat.documents || []).map(d => (
                            <Chip
                              key={d.document_id}
                              size="small"
                              icon={<FileText size={11} />}
                              label={d.filename}
                              variant="outlined"
                              color="warning"
                              sx={{ height: 20, maxWidth: 200, "& .MuiChip-label": { fontSize: 10 } }}
                            />
                          ))}
                        </Stack>
                      )}
                    </Box>
                  }
                />
              </ListItemButton>
            ))}
          </List>
        )}
      </Drawer>
    </Paper>
  );
};

const ChatAssistant = React.forwardRef(ChatAssistantComponent);
ChatAssistant.displayName = "ChatAssistant";

export default ChatAssistant;
