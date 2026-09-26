import React, { useState, useRef, useEffect, useCallback } from "react";
import { useToast } from "./ToastProvider";
import {
  Send, Square, MessageSquare, Menu as MenuIcon, SquarePen,
  Maximize2, Minimize2, PanelRightClose, Trash2, X, ChevronRight,
  ArrowDown, Copy, Check,
} from "lucide-react";
import {
  Box, Paper, Stack, Typography, IconButton, Tooltip, TextField, Badge,
} from "@mui/material";
import ModelSelector from "./ModelSelector";
import AgentMessage from "./agent/AgentMessage";
import AgentRunMessage from "./agent/AgentRunMessage";
import MarkdownLite from "./agent/MarkdownLite";
import { FyntracMark } from "./fyntracMark";
import { runAgentPipeline, generateMessageId } from "../agent/agentPipeline";
import { detectFunctionMention, getExplanation, formatForChat, detectConceptMention, getConcept, formatConceptForChat } from "../agent/testing/explanationStore";
import "./ChatAssistant.css";

// Panel widths, shared with the dock that slides it in and out.
export const CHAT_PANEL_WIDTH = 504;
export const CHAT_PANEL_WIDTH_EXPANDED = 760;

// Icons down the panel's left rail.
const railBtnSx = {
  width: 30, height: 30, borderRadius: 2, color: "#6B7280",
  "&:hover": { bgcolor: "#F1F3F9", color: "#14213D" },
};

// ── Multi-conversation store (localStorage) ────────────────────────────────
// Each chat: { id, title, updatedAt, sessionId, messages }
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

// "11 minutes ago" — how the conversation list dates a thread.
const fmtAgo = (ts) => {
  if (!ts) return "";
  const mins = Math.round(Math.max(0, Date.now() - ts) / 60000);
  if (mins < 1) return "just now";
  const plural = (n, unit) => `${n} ${unit}${n === 1 ? "" : "s"} ago`;
  if (mins < 60) return plural(mins, "minute");
  const hours = Math.round(mins / 60);
  if (hours < 24) return plural(hours, "hour");
  const days = Math.round(hours / 24);
  if (days < 7) return plural(days, "day");
  const d = new Date(ts);
  return d.toLocaleDateString([], d.getFullYear() === new Date().getFullYear()
    ? { month: "short", day: "numeric" }
    : { month: "short", day: "numeric", year: "numeric" });
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

const ChatAssistantComponent = ({ dslFunctions, events, onInsertCode, onOverwriteCode, editorCode, consoleOutput, editorRef, monacoRef, providerRefreshKey, uiContext, onAgentDataChange, onClose, expanded = false, onToggleExpanded }, ref) => {
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
  const [showScrollBtn, setShowScrollBtn] = useState(false);
  const [copiedId, setCopiedId] = useState(null);
  const scrollRef = useRef(null);
  const inputRef = useRef(null);
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
    if (!persistable.length) return;
    const all = loadChats();
    const entry = {
      id: currentChatId,
      title: chatTitle(persistable),
      updatedAt: Date.now(),
      sessionId: sessionId || null,
      messages: persistable,
    };
    const idx = all.findIndex(c => c.id === currentChatId);
    if (idx >= 0) all[idx] = entry; else all.unshift(entry);
    all.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
    saveChats(all);
    setChats(all);
  }, [messages, sessionId, currentChatId]);

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

    const handleSendWithMessage = async (userMessage) => {
    setLoading(true);

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

  // The header's second line names the thread, the way a product copilot
  // titles the conversation you are in.
  const conversationLabel = visibleMessages.length === 0
    ? "New conversation"
    : chatTitle(messages);

  // The panel's two chrome controls, shared by the conversation and the
  // Chats list so the header keeps its shape between the two views.
  const panelControls = (
    <>
      {onToggleExpanded && (
        <Tooltip title={expanded ? "Shrink panel" : "Expand panel"}>
          <IconButton size="small" onClick={onToggleExpanded}
            data-testid="chat-expand-button"
            aria-label={expanded ? "Shrink panel" : "Expand panel"}
            sx={railBtnSx}>
            {expanded ? <Minimize2 size={16} /> : <Maximize2 size={16} />}
          </IconButton>
        </Tooltip>
      )}
      {onClose && (
        <Tooltip title={"Close Copilot (Ctrl/⌘ + J)"}>
          <IconButton size="small" onClick={onClose}
            data-testid="chat-close-button" aria-label="Close Copilot"
            sx={railBtnSx}>
            <PanelRightClose size={17} />
          </IconButton>
        </Tooltip>
      )}
    </>
  );

  const panelSx = {
    width: expanded ? CHAT_PANEL_WIDTH_EXPANDED : CHAT_PANEL_WIDTH,
    height: "100%", display: "flex",
    borderRadius: 0, borderLeft: "1px solid #E9ECEF", bgcolor: "#fff",
    transition: "width 260ms cubic-bezier(0.22, 1, 0.36, 1)",
    position: "relative", overflow: "hidden",
  };

  // ── Chats ─────────────────────────────────────────────────────────────
  // A view of the panel rather than a drawer over it: the rail steps aside
  // and an × returns you to the conversation you came from.
  if (historyOpen) {
    return (
      <Paper elevation={0} data-testid="chat-assistant" sx={panelSx}>
        <Box sx={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column" }}>
          <Box sx={{
            px: 1.5, py: 1.5, display: "flex", alignItems: "center", gap: 1,
            borderBottom: "1px solid #F1F3F5", flexShrink: 0,
          }}>
            <Tooltip title="Back to the conversation">
              <IconButton size="small" onClick={() => setHistoryOpen(false)}
                data-testid="chat-history-close" aria-label="Back to the conversation"
                sx={railBtnSx}>
                <X size={18} />
              </IconButton>
            </Tooltip>
            <Typography component="div" className="copilot-title"
              sx={{ flex: 1, minWidth: 0, fontSize: 16 }}>
              Chats
            </Typography>
            {panelControls}
          </Box>

          <Box className="chat-scroll" data-testid="chat-history-list"
            sx={{ flex: 1, overflowY: "auto", px: 1.5, py: 1.5 }}>
            {chats.length === 0 ? (
              <Stack alignItems="center" sx={{ pt: 6, px: 3, textAlign: "center" }}>
                <MessageSquare size={26} color="#C4C8D4" />
                <Typography className="copilot-prose" sx={{ mt: 1.5, color: "#6b7280" }}>
                  No conversations yet. Your chats are saved here automatically.
                </Typography>
              </Stack>
            ) : chats.map(chat => {
              const current = chat.id === currentChatId;
              return (
                <Box
                  key={chat.id}
                  role="button"
                  tabIndex={0}
                  onClick={() => handleLoadChat(chat)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      handleLoadChat(chat);
                    }
                  }}
                  sx={{
                    display: "flex", alignItems: "center", gap: 1.25,
                    px: 1.5, py: 1.25, mb: 0.5, borderRadius: "10px",
                    cursor: "pointer", transition: "background 0.13s ease",
                    bgcolor: current ? "#EAECFD" : "transparent",
                    "&:hover": { bgcolor: current ? "#E1E4FC" : "#F5F6FA" },
                    "&:hover .chat-row-del": { opacity: 1 },
                    "&:focus-visible": { outline: "2px solid #5B5FED", outlineOffset: 2 },
                  }}
                >
                  <MessageSquare size={17} color="#5B5FED" style={{ flexShrink: 0 }} />
                  <Box sx={{ flex: 1, minWidth: 0 }}>
                    <Typography noWrap sx={{
                      fontSize: 13.5, fontWeight: 600, letterSpacing: "-0.008em",
                      color: "#14213D",
                    }}>
                      {chat.title || "New conversation"}
                    </Typography>
                    </Box>
                  <Typography sx={{ fontSize: 12, color: "#6B7280", flexShrink: 0 }}>
                    {fmtAgo(chat.updatedAt)}
                  </Typography>
                  <IconButton
                    className="chat-row-del" size="small"
                    onClick={(e) => handleDeleteChat(e, chat.id)}
                    aria-label="Delete conversation" title="Delete conversation"
                    sx={{ opacity: 0, transition: "opacity 0.13s", p: 0.25, color: "#9AA0AA" }}
                  >
                    <Trash2 size={14} />
                  </IconButton>
                  <ChevronRight size={16} color="#9AA0AA" style={{ flexShrink: 0 }} />
                </Box>
              );
            })}
          </Box>
        </Box>
      </Paper>
    );
  }

  // -- Panel ---------------------------------------------------------------
  // There is no collapsed stub: dismissing the Copilot is the parent's job,
  // and the floating Fyntrac button brings it back.
  return (
    <Paper
      elevation={0}
      data-testid="chat-assistant"
      sx={panelSx}
    >
      {/* Left icon rail */}
      <Box sx={{
        width: 44, flexShrink: 0, display: "flex", flexDirection: "column",
        alignItems: "center", pt: 1.5, gap: 0.5,
        borderRight: "1px solid #F1F3F5",
      }}>
        <Tooltip title="Conversations" placement="right">
          <IconButton size="small" onClick={() => setHistoryOpen(true)}
            data-testid="chat-history-button" aria-label="Conversations"
            sx={railBtnSx}>
            <Badge badgeContent={chats.length} color="primary" max={99}
              sx={{ "& .MuiBadge-badge": { fontSize: 9, height: 14, minWidth: 14 } }}>
              <MenuIcon size={17} />
            </Badge>
          </IconButton>
        </Tooltip>
        <Tooltip title="New conversation" placement="right">
          <span>
            <IconButton size="small" onClick={handleNewChat} disabled={loading}
              data-testid="new-chat-button" aria-label="New conversation"
              sx={railBtnSx}>
              <SquarePen size={16} />
            </IconButton>
          </span>
        </Tooltip>
      </Box>

      {/* Conversation column */}
      <Box sx={{ flex: 1, minWidth: 0, display: "flex", flexDirection: "column" }}>
        {/* Header */}
        <Box sx={{
          px: 2, py: 1.5, display: "flex", alignItems: "flex-start", gap: 0.5,
          borderBottom: "1px solid #F1F3F5", flexShrink: 0,
        }}>
          <Box sx={{ flex: 1, minWidth: 0, pt: 0.25 }}>
            <Typography component="div" className="copilot-title" noWrap>
              Fyntrac Copilot
            </Typography>
            <Typography component="div" className="copilot-subtitle" noWrap>
              {conversationLabel}
            </Typography>
          </Box>
          {panelControls}
        </Box>

        {/* Messages */}
        <Box ref={scrollRef} onScroll={handleScroll}
          role="log" aria-live="polite" aria-relevant="additions text"
          aria-label="Conversation with Fyntrac Copilot"
          className="chat-scroll"
          sx={{ flex: 1, overflowY: "auto", px: 2.5, py: 1.5, bgcolor: "#fff", position: "relative" }}>
          {visibleMessages.length === 0 && (
            <Stack alignItems="center" sx={{ pt: 4, px: 2, textAlign: "center" }}>
              <FyntracMark size={42} />
              <Typography sx={{
                mt: 2, fontSize: 19, fontWeight: 650, letterSpacing: "-0.02em",
                color: "#14213d",
              }}>
                Ask me anything
              </Typography>
              <Typography className="copilot-prose" sx={{ mt: 1, maxWidth: 400, color: "#6b7280" }}>
                Get help with Fyntrac accounting rules — DSL functions, events,
                schedules and the Rule Builder. Choose a suggestion below, or
                ask what’s on your mind.
              </Typography>
              <Stack spacing={1} sx={{ width: "100%", maxWidth: 440, pt: 3 }}>
                {[
                "What does pmt() do? Show me with sample numbers.",
                "Walk me through building a loan amortization rule",
                "How do I add a Schedule step in the Rule Builder?",
              ].map((q, i) => (
                  <button
                    key={i}
                    type="button"
                    className="copilot-suggestion"
                    onClick={() => {
                      setInput(q);
                      if (inputRef.current) inputRef.current.focus();
                    }}
                  >
                    {q}
                  </button>
                ))}
              </Stack>
            </Stack>
          )}

          <Box sx={{ display: "flex", flexDirection: "column" }}>
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

              if (msg.role === "user") {
                return (
                  <React.Fragment key={idx}>
                    {dateSep}
                    <Box
                      className="chat-msg-in"
                      sx={{
                        display: "flex", flexDirection: "column", alignItems: "flex-end",
                        mt: grouped ? 0.75 : 2.5,
                        "&:hover .msg-copy": { opacity: 1 },
                      }}
                    >
                      <Box sx={{
                        maxWidth: "82%", px: 2, py: 1.25,
                        bgcolor: "#EAECFD", color: "#1F2544",
                        borderRadius: "20px",
                        fontSize: 14.5, lineHeight: 1.55, letterSpacing: "-0.008em",
                        whiteSpace: "pre-wrap", wordBreak: "break-word",
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
                    <Box className="chat-msg-in" sx={{ width: "100%", mt: 1.5 }}>
                      <Box sx={{ mb: 1 }}><FyntracMark size={22} /></Box>
                      <Box className="copilot-answer">
                        <AgentMessage
                          messageId={msg.messageId}
                          onInsertCode={onInsertCode}
                          onOverwriteCode={onOverwriteCode}
                        />
                      </Box>
                    </Box>
                  </React.Fragment>
                );
              }

              if (msg.role === "assistant") {
                return (
                  <React.Fragment key={idx}>
                    {dateSep}
                    <Box className="chat-msg-in" sx={{
                      width: "100%", minWidth: 0,
                      mt: grouped ? 0.5 : 1.5,
                      "&:hover .msg-copy": { opacity: 1 },
                    }}>
                    {!grouped && (
                      <Box sx={{ mb: 1 }}><FyntracMark size={22} /></Box>
                    )}
                    <Box sx={{ minWidth: 0 }}>
                      {msg.error_type ? (
                        <Paper elevation={0} sx={{
                          px: 1.75, py: 1.125, borderRadius: 2,
                          border: "1px solid #FBD5D5", bgcolor: "#FEF6F6",
                          display: "flex", gap: 0.75, alignItems: "center",
                        }}>
                          <Typography color="error.main"
                            sx={{ fontSize: 14.5, lineHeight: 1.6, letterSpacing: "-0.008em" }}>
                            {msg.error_message || msg.content}
                          </Typography>
                        </Paper>
                      ) : (
                        // No card: the reply is prose on the page, the way the
                        // product copilots people already use present it.
                        <Box className="copilot-answer" sx={{ wordBreak: "break-word" }}>
                          <MarkdownLite text={msg.content} style={{ fontSize: 14.5 }} />
                        </Box>
                      )}
                      <Stack direction="row" alignItems="center" spacing={0.5} sx={{ mt: 0.25 }}>
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
              <Box className="chat-msg-in" sx={{ width: "100%", mt: 1.5 }}>
                <Box sx={{ mb: 1 }}><FyntracMark size={22} /></Box>
                <Box aria-label="Assistant is typing" sx={{ py: 0.5 }}>
                  <Box className="chat-typing" aria-hidden="true">
                    <span /><span /><span />
                  </Box>
                </Box>
              </Box>
            )}
          </Box>

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

        {/* Composer: model picker, then the input card and its disclaimer */}
        <Box sx={{ px: 2, pt: 1, pb: 1.25, flexShrink: 0, bgcolor: "#fff" }}>
          <Box sx={{ mb: 1 }}>
            <ModelSelector onModelChange={handleModelChange} refreshKey={providerRefreshKey} />
          </Box>

          {/* Input card: textarea above, actions on the row beneath. */}
          <Box sx={{
            border: "1px solid #E3E5EC", borderRadius: "14px",
            px: 1.5, pt: 1.25, pb: 0.75, bgcolor: "#fff",
            transition: "border-color 0.15s ease, box-shadow 0.15s ease",
            "&:focus-within": {
              borderColor: "#5B5FED",
              boxShadow: "0 0 0 3px rgba(91,95,237,0.12)",
            },
          }}>
            <TextField
              inputRef={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder={loading ? "Generating…" : "Ask Copilot…"}
              fullWidth
              multiline
              maxRows={6}
              variant="standard"
              disabled={loading}
              data-testid="chat-input"
              InputProps={{
                disableUnderline: true,
                sx: {
                  p: 0, fontSize: 13.5, lineHeight: 1.6,
                  letterSpacing: "-0.006em", color: "#1f2937",
                },
              }}
            />

            <Stack direction="row" alignItems="center" sx={{ mt: 0.75 }}>
              <Box sx={{ flex: 1 }} />

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
                          bgcolor: active ? "#5B5FED" : "#F1F3F5",
                          color: active ? "#fff" : "#ADB5BD",
                          boxShadow: active ? "0 2px 8px rgba(91,95,237,0.30)" : "none",
                          transition: "background 0.15s, box-shadow 0.15s, transform 0.1s",
                          "&:hover": {
                            bgcolor: active ? "#4346C8" : "#F1F3F5",
                            transform: active ? "scale(1.06)" : "none",
                          },
                          "&.Mui-disabled": { bgcolor: "#F1F3F5", color: "#CED4DA" },
                        }}
                      >
                        {stopHandler || loading ? <Square size={14} /> : <Send size={14} />}
                      </IconButton>
                    );
                  })()}
                </span>
              </Tooltip>
            </Stack>
          </Box>

          <Typography sx={{
            display: "block", mt: 0.75, textAlign: "center",
            fontSize: 10.5, lineHeight: 1.45, color: "#9AA0AA",
            letterSpacing: "-0.003em",
          }}>
            Shift+Enter for a new line. Fyntrac Copilot can make mistakes —
            review generated rules before you deploy.
          </Typography>
        </Box>
      </Box>

    </Paper>
  );
};

const ChatAssistant = React.forwardRef(ChatAssistantComponent);
ChatAssistant.displayName = "ChatAssistant";

export default ChatAssistant;
