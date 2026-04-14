import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Box, Typography, Card, CardContent, Button, IconButton, Chip,
  CircularProgress, Alert, Tooltip, Divider,
} from "@mui/material";
import { Trash2, Edit3, Calculator, GitBranch, Repeat, Database, Clock, Upload, Play, GripVertical } from "lucide-react";
import { API } from "../../config";

const RULE_TYPE_META = {
  simple_calc: { label: 'Calculation', color: '#5B5FED', icon: Calculator },
  conditional: { label: 'Conditional', color: '#FF9800', icon: GitBranch },
  iteration: { label: 'Iteration', color: '#00BCD4', icon: Repeat },
  collect: { label: 'Collect', color: '#8BC34A', icon: Database },
};

const SavedRules = ({ onEditRule, refreshKey, onLoadToEditor, onPlayAll }) => {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [deleting, setDeleting] = useState(null);
  const [playing, setPlaying] = useState(false);

  const loadRules = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${API}/saved-rules`);
      const data = await res.json();
      setRules(Array.isArray(data) ? data : []);
    } catch (err) {
      setError(err.message || 'Failed to load saved rules');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadRules();
  }, [loadRules, refreshKey]);

  const handleDelete = useCallback(async (rule) => {
    if (!window.confirm(`Delete rule "${rule.name}"? This cannot be undone.`)) return;
    setDeleting(rule.id);
    try {
      const res = await fetch(`${API}/saved-rules/${rule.id}`, { method: 'DELETE' });
      if (res.ok) {
        setRules(prev => prev.filter(r => r.id !== rule.id));
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setDeleting(null);
    }
  }, []);

  const formatDate = (iso) => {
    if (!iso) return '';
    try {
      return new Date(iso).toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' });
    } catch { return iso; }
  };

  // Sort rules by priority (lower number = higher priority = first)
  const sortedRules = [...rules].sort((a, b) => {
    const pa = a.priority ?? Infinity;
    const pb = b.priority ?? Infinity;
    return pa - pb;
  });

  // ── Drag-and-drop reordering ──
  const dragItem = useRef(null);
  const dragOverItem = useRef(null);

  const handleDragStart = useCallback((idx) => {
    dragItem.current = idx;
  }, []);

  const handleDragOver = useCallback((e, idx) => {
    e.preventDefault();
    dragOverItem.current = idx;
  }, []);

  const handleDrop = useCallback(async (e) => {
    e.preventDefault();
    const from = dragItem.current;
    const to = dragOverItem.current;
    if (from === null || to === null || from === to) return;

    // Reorder the sorted list
    const reordered = [...sortedRules];
    const [moved] = reordered.splice(from, 1);
    reordered.splice(to, 0, moved);

    // Assign new sequential priorities starting from 1
    const order = reordered.map((r, idx) => ({ id: r.id, priority: idx + 1 }));

    // Optimistically update local state
    const updatedRules = rules.map(r => {
      const match = order.find(o => o.id === r.id);
      return match ? { ...r, priority: match.priority } : r;
    });
    setRules(updatedRules);

    // Persist to backend
    try {
      await fetch(`${API}/saved-rules/reorder`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ order }),
      });
      // Reload from backend to ensure consistency
      await loadRules();
    } catch (err) {
      setError('Failed to save new order: ' + (err.message || ''));
    }

    dragItem.current = null;
    dragOverItem.current = null;
  }, [sortedRules, rules, loadRules]);

  const handleLoadToEditor = useCallback(() => {
    if (onLoadToEditor && sortedRules.length > 0) {
      const combinedCode = sortedRules
        .map(r => r.generatedCode || '')
        .filter(Boolean)
        .join('\n\n');
      onLoadToEditor(combinedCode);
    }
  }, [onLoadToEditor, sortedRules]);

  const handlePlayAll = useCallback(async () => {
    if (!onPlayAll || sortedRules.length === 0) return;
    setPlaying(true);
    setError(null);
    try {
      const combinedCode = sortedRules
        .map(r => r.generatedCode || '')
        .filter(Boolean)
        .join('\n\n');
      const pdRes = await fetch(`${API}/event-data/posting-dates`);
      const pdData = await pdRes.json();
      const dates = pdData?.posting_dates || [];
      const payload = { dsl_code: combinedCode };
      if (dates.length >= 1) payload.posting_date = dates[0];
      const res = await fetch(`${API}/dsl/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (res.ok && data.success) {
        onPlayAll({
          transactions: data.transactions || [],
          printOutputs: data.print_outputs || [],
        });
      } else {
        setError(data.error || data.detail || 'Execution failed');
      }
    } catch (err) {
      setError(err.message || 'Execution failed');
    } finally {
      setPlaying(false);
    }
  }, [onPlayAll, sortedRules]);

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', p: 6 }}>
        <CircularProgress size={32} />
      </Box>
    );
  }

  return (
    <Box sx={{ p: 2, height: '100%', overflow: 'auto' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
        <Typography variant="h6" fontWeight={600}>Saved Rules</Typography>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Typography variant="caption" color="text.secondary">{rules.length} rule{rules.length !== 1 ? 's' : ''}</Typography>
          {rules.length > 0 && (
            <>
              <Tooltip title="Load all rules to editor (sorted by priority)">
                <IconButton size="small" onClick={handleLoadToEditor} sx={{ color: '#5B5FED' }}>
                  <Upload size={16} />
                </IconButton>
              </Tooltip>
              <Tooltip title="Play all rules (sorted by priority)">
                <IconButton size="small" onClick={handlePlayAll} disabled={playing} sx={{ color: '#4CAF50' }}>
                  {playing ? <CircularProgress size={16} /> : <Play size={16} />}
                </IconButton>
              </Tooltip>
            </>
          )}
        </Box>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>{error}</Alert>
      )}

      {rules.length === 0 && !error && (
        <Box sx={{ textAlign: 'center', py: 6, color: 'text.secondary' }}>
          <Calculator size={40} style={{ margin: '0 auto 12px', opacity: 0.3 }} />
          <Typography variant="body1" fontWeight={500}>No saved rules yet</Typography>
          <Typography variant="body2" sx={{ mt: 0.5 }}>
            Build a rule using the Rule Builder and click Save Rule to see it here.
          </Typography>
        </Box>
      )}

      {sortedRules.map((rule, sortIdx) => {
        const meta = RULE_TYPE_META[rule.ruleType] || RULE_TYPE_META.simple_calc;
        const Icon = meta.icon;
        const varCount = (rule.variables || []).filter(v => v.name).length;
        const hasTxn = rule.outputs?.createTransaction;
        const priority = rule.priority ?? '—';

        return (
          <Card
            key={rule.id}
            draggable
            onDragStart={() => handleDragStart(sortIdx)}
            onDragOver={(e) => handleDragOver(e, sortIdx)}
            onDrop={handleDrop}
            sx={{
              mb: 1.5, cursor: 'pointer', transition: 'all 0.15s',
              border: '1px solid #E9ECEF',
              '&:hover': { borderColor: '#5B5FED', boxShadow: '0 2px 8px rgba(91,95,237,0.12)' },
            }}
            onClick={() => onEditRule(rule)}
          >
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
              <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1.5 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', cursor: 'grab', pt: 1, flexShrink: 0 }}
                  onMouseDown={(e) => e.stopPropagation()}>
                  <GripVertical size={16} color="#ADB5BD" />
                </Box>
                <Box sx={{ width: 36, height: 36, borderRadius: 1, bgcolor: `${meta.color}14`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, mt: 0.25 }}>
                  <Icon size={18} color={meta.color} />
                </Box>
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                    <Typography variant="body1" fontWeight={600} noWrap>{rule.name}</Typography>
                    <Chip size="small" label={meta.label} sx={{ fontSize: '0.6875rem', height: 20, bgcolor: `${meta.color}18`, color: meta.color, fontWeight: 600 }} />
                    <Chip size="small" label={`P${priority}`} sx={{ fontSize: '0.625rem', height: 18, bgcolor: '#E3F2FD', color: '#1565C0', fontWeight: 600 }} />
                    {hasTxn && <Chip size="small" label="Txn" sx={{ fontSize: '0.625rem', height: 18 }} variant="outlined" />}
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                    {varCount > 0 && (
                      <Typography variant="caption" color="text.secondary">{varCount} step{varCount !== 1 ? 's' : ''}</Typography>
                    )}
                    {rule.updated_at && (
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                        <Clock size={10} /> {formatDate(rule.updated_at)}
                      </Typography>
                    )}
                  </Box>
                  {rule.commentText && (
                    <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }} noWrap>
                      {rule.commentText}
                    </Typography>
                  )}
                </Box>
                <Box sx={{ display: 'flex', gap: 0.5, flexShrink: 0 }}>
                  <Tooltip title="Edit rule">
                    <IconButton size="small" onClick={(e) => { e.stopPropagation(); onEditRule(rule); }} sx={{ color: '#5B5FED' }}>
                      <Edit3 size={16} />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Delete rule">
                    <IconButton size="small" onClick={(e) => { e.stopPropagation(); handleDelete(rule); }}
                      disabled={deleting === rule.id}
                      sx={{ color: '#F44336' }}>
                      {deleting === rule.id ? <CircularProgress size={14} /> : <Trash2 size={16} />}
                    </IconButton>
                  </Tooltip>
                </Box>
              </Box>
            </CardContent>
          </Card>
        );
      })}
    </Box>
  );
};

export default SavedRules;
