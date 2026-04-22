import React, { useState, useEffect, useCallback, useRef } from "react";
import {
  Box, Typography, Card, CardContent, Button, IconButton, Chip, TextField,
  CircularProgress, Alert, Tooltip, Divider,
  Dialog, DialogTitle, DialogContent, DialogContentText, DialogActions,
} from "@mui/material";
import { Trash2, Edit3, Calculator, GitBranch, Repeat, Database, Clock, Play, GripVertical, BookmarkPlus, RotateCcw, Code, Calendar, Copy } from "lucide-react";
import { API } from "../../config";

const RULE_TYPE_META = {
  simple_calc: { label: 'Calculation', color: '#5B5FED', icon: Calculator },
  conditional: { label: 'Conditional', color: '#FF9800', icon: GitBranch },
  iteration: { label: 'Iteration', color: '#00BCD4', icon: Repeat },
  collect: { label: 'Collect', color: '#8BC34A', icon: Database },
  custom_code: { label: 'Custom Code', color: '#9C27B0', icon: Code },
  schedule: { label: 'Schedule', color: '#2196F3', icon: Calendar },
};

const SavedRules = ({ onEditRule, onEditSchedule, refreshKey, onPlayAll, onClearAll }) => {
  const [rules, setRules] = useState([]);
  const [schedules, setSchedules] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [deleting, setDeleting] = useState(null);
  const [deleteTarget, setDeleteTarget] = useState(null);
  const [playing, setPlaying] = useState(false);
  const [showSaveTemplate, setShowSaveTemplate] = useState(false);
  const [templateName, setTemplateName] = useState('');
  const [templateDesc, setTemplateDesc] = useState('');
  const [templateCategory, setTemplateCategory] = useState('');
  const [savingTemplate, setSavingTemplate] = useState(false);
  const [templateResult, setTemplateResult] = useState(null);
  // Persist the last saved/loaded template id so repeat saves overwrite without showing the modal.
  // Initialised from localStorage so it survives tab switches (SavedRules unmounts/remounts).
  // When a template is loaded from the library, Dashboard.js writes the id to localStorage
  // (in handleGeneratedCode) before switching away, so this picks it up on remount.
  const [savedTemplateId, setSavedTemplateId] = useState(() => {
    try { return localStorage.getItem('savedRulesTemplateId') || null; } catch { return null; }
  });
  const [showClearAll, setShowClearAll] = useState(false);
  const [clearing, setClearing] = useState(false);
  const [duplicateTarget, setDuplicateTarget] = useState(null);
  const [dupName, setDupName] = useState('');
  const [dupPriority, setDupPriority] = useState('');
  const [duplicating, setDuplicating] = useState(false);

  const loadRules = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      // Use ?summary=1 to exclude generatedCode from list responses — significantly
      // reduces payload size when there are many rules with large generated code.
      const [rulesRes, schedsRes] = await Promise.all([
        fetch(`${API}/saved-rules?summary=1`),
        fetch(`${API}/saved-schedules?summary=1`).catch(() => ({ ok: true, json: async () => [] })),
      ]);
      const rulesData = await rulesRes.json();
      const schedsData = await schedsRes.json();
      setRules(Array.isArray(rulesData) ? rulesData : []);
      setSchedules(Array.isArray(schedsData) ? schedsData : []);
    } catch (err) {
      setError(err.message || 'Failed to load saved rules');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadRules();
  }, [loadRules, refreshKey]);

  const handleDelete = useCallback(async (item) => {
    setDeleting(item.id);
    setDeleteTarget(null);
    try {
      const endpoint = item._isSchedule ? 'saved-schedules' : 'saved-rules';
      const res = await fetch(`${API}/${endpoint}/${item.id}`, { method: 'DELETE' });
      if (res.ok) {
        if (item._isSchedule) {
          setSchedules(prev => prev.filter(s => s.id !== item.id));
        } else {
          setRules(prev => prev.filter(r => r.id !== item.id));
        }
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

  const handleDuplicate = useCallback(async () => {
    if (!duplicateTarget || !dupName.trim() || dupPriority === '') return;
    setDuplicating(true);
    try {
      const endpoint = duplicateTarget._isSchedule ? `${API}/saved-schedules` : `${API}/saved-rules`;
      const payload = {
        ...duplicateTarget,
        id: undefined,
        name: dupName.trim(),
        priority: Number(dupPriority),
        created_at: undefined,
        updated_at: undefined,
      };
      delete payload.id;
      delete payload.created_at;
      delete payload.updated_at;
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (res.ok) {
        await loadRules();
        setDuplicateTarget(null);
      } else {
        const data = await res.json();
        setError(data.detail || data.error || 'Duplicate failed');
      }
    } catch (err) {
      setError(err.message || 'Duplicate failed');
    } finally {
      setDuplicating(false);
    }
  }, [duplicateTarget, dupName, dupPriority, loadRules]);

  // Merge rules and schedules, sort by priority
  const allItems = [
    ...rules.map(r => ({ ...r, _isSchedule: false })),
    ...schedules.map(s => ({ ...s, _isSchedule: true, ruleType: 'schedule' })),
  ];
  const sortedRules = [...allItems].sort((a, b) => {
    const pa = a.priority ?? Infinity;
    const pb = b.priority ?? Infinity;
    return pa - pb;
  });
  const totalCount = rules.length + schedules.length;

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

  const handlePlayAll = useCallback(async () => {
    if (!onPlayAll || sortedRules.length === 0) return;
    setPlaying(true);
    setError(null);
    try {
      // Use the /combined-code endpoint which handles dep-stripping on the server,
      // so we don't need generatedCode in the local list state.
      const codeRes = await fetch(`${API}/combined-code`);
      const codeData = await codeRes.json();
      const combinedCode = codeData?.code || '';
      if (!combinedCode) {
        setError('No generated code found. Save your rules first.');
        return;
      }
      const pdRes = await fetch(`${API}/event-data/posting-dates`);
      const pdData = await pdRes.json();
      const dates = pdData?.posting_dates || [];
      if (dates.length === 0) dates.push(null);
      let allTransactions = [];
      let allPrintOutputs = [];
      let lastError = null;
      for (const date of dates) {
        const payload = { dsl_code: combinedCode };
        if (date) payload.posting_date = date;
        const res = await fetch(`${API}/dsl/run`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (res.ok && data.success) {
          allTransactions = allTransactions.concat(data.transactions || []);
          allPrintOutputs = allPrintOutputs.concat(data.print_outputs || []);
        } else {
          lastError = data.error || data.detail || 'Execution failed';
        }
      }
      if (allTransactions.length > 0 || allPrintOutputs.length > 0) {
        const ruleNames = sortedRules.map(r => r.name).filter(Boolean);
        const baseName = ruleNames.length > 0 ? ruleNames[0].replace(/\s*-\s*(Parameters|Schedule|Iteration|Transactions|Conditional)$/i, '') : '';
        onPlayAll({
          transactions: allTransactions,
          printOutputs: allPrintOutputs,
          templateName: baseName,
        });
      } else if (lastError) {
        setError(lastError);
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
          <Typography variant="caption" color="text.secondary">{totalCount} rule{totalCount !== 1 ? 's' : ''}</Typography>
          {totalCount > 0 && (
            <>
              <Tooltip title="Play all rules (sorted by priority)">
                <IconButton size="small" onClick={handlePlayAll} disabled={playing} sx={{ color: '#4CAF50' }}>
                  {playing ? <CircularProgress size={16} /> : <Play size={16} />}
                </IconButton>
              </Tooltip>
              <Tooltip title={(() => { try { return localStorage.getItem('savedRulesTemplateId') || null; } catch { return null; } })() ? 'Update saved template' : 'Save all rules as a reusable template'}>
                <IconButton size="small" onClick={async () => {
                  // Always read localStorage fresh — state may be stale if template was deleted
                  // while this component was mounted (e.g. user visited Templates tab and came back).
                  const currentTemplateId = (() => {
                    try { return localStorage.getItem('savedRulesTemplateId') || null; } catch { return null; }
                  })();
                  // Sync state with localStorage if they diverged
                  if (currentTemplateId !== savedTemplateId) { setSavedTemplateId(currentTemplateId); }
                  if (currentTemplateId) {
                    // Overwrite existing template directly — no modal.
                    // Fetch full rules (with generatedCode) on demand.
                    setSavingTemplate(true);
                    try {
                      const [fullRulesRes, codeRes] = await Promise.all([
                        fetch(`${API}/saved-rules`),
                        fetch(`${API}/combined-code`),
                      ]);
                      const fullRules = await fullRulesRes.json();
                      const codeData = await codeRes.json();
                      const combinedCode = codeData?.code || '';
                      const ruleSummaries = (Array.isArray(fullRules) ? fullRules : []).map(r => ({
                        name: r.name, priority: r.priority, ruleType: r.ruleType,
                        generatedCode: r.generatedCode, variables: r.variables || [],
                        conditions: r.conditions || [], elseFormula: r.elseFormula || '',
                        conditionResultVar: r.conditionResultVar || 'result',
                        iterations: r.iterations || [], iterConfig: r.iterConfig || {},
                        outputs: r.outputs || {}, customCode: r.customCode || '',
                        steps: r.steps || [],
                      }));
                      const res = await fetch(`${API}/user-templates/${currentTemplateId}`, {
                        method: 'PUT',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ rules: ruleSummaries, combinedCode }),
                      });
                      if (!res.ok) {
                        // Template no longer exists (404) or any other failure — fall back to modal
                        try { localStorage.removeItem('savedRulesTemplateId'); } catch { /* ignore */ }
                        setSavedTemplateId(null);
                        setShowSaveTemplate(true);
                        setTemplateResult(null);
                      }
                      // Silently succeed on 2xx — no toast needed
                    } catch {
                      // Network / parse error — fall back to modal so user can retry
                      try { localStorage.removeItem('savedRulesTemplateId'); } catch { /* ignore */ }
                      setSavedTemplateId(null);
                      setShowSaveTemplate(true);
                      setTemplateResult(null);
                    } finally { setSavingTemplate(false); }
                  } else {
                    setShowSaveTemplate(true); setTemplateResult(null);
                  }
                }} disabled={savingTemplate} sx={{ color: '#FF9800' }}>
                  {savingTemplate ? <CircularProgress size={16} /> : <BookmarkPlus size={16} />}
                </IconButton>
              </Tooltip>
              <Divider orientation="vertical" flexItem sx={{ mx: 0.5 }} />
              <Tooltip title="Clear everything — editor, console, preview & rules">
                <IconButton size="small" onClick={() => setShowClearAll(true)} sx={{ color: '#F44336' }}>
                  <RotateCcw size={16} />
                </IconButton>
              </Tooltip>
            </>
          )}
        </Box>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>{error}</Alert>
      )}

      {totalCount === 0 && !error && (
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
        const varCount = rule._isSchedule ? (rule.config?.columns?.length || 0) : (rule.variables || []).filter(v => v.name).length;
        const hasTxn = rule._isSchedule ? rule.config?.createTxn : rule.outputs?.createTransaction;
        const priority = rule.priority ?? '—';
        const handleClick = () => rule._isSchedule ? onEditSchedule?.(rule) : onEditRule(rule);

        return (
          <Card
            key={`${rule._isSchedule ? 's' : 'r'}-${rule.id}`}
            draggable
            onDragStart={() => handleDragStart(sortIdx)}
            onDragOver={(e) => handleDragOver(e, sortIdx)}
            onDrop={handleDrop}
            sx={{
              mb: 1.5, cursor: 'pointer', transition: 'all 0.15s',
              border: '1px solid #E9ECEF',
              '&:hover': { borderColor: meta.color, boxShadow: `0 2px 8px ${meta.color}1F` },
            }}
            onClick={handleClick}
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
                      <Typography variant="caption" color="text.secondary">
                        {rule._isSchedule ? `${varCount} column${varCount !== 1 ? 's' : ''}` : `${varCount} step${varCount !== 1 ? 's' : ''}`}
                      </Typography>
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
                  <Tooltip title={rule._isSchedule ? 'Edit schedule' : 'Edit rule'}>
                    <IconButton size="small" onClick={(e) => { e.stopPropagation(); handleClick(); }} sx={{ color: meta.color }}>
                      <Edit3 size={16} />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title={rule._isSchedule ? 'Duplicate schedule' : 'Duplicate rule'}>
                    <IconButton size="small" onClick={(e) => {
                      e.stopPropagation();
                      setDuplicateTarget(rule);
                      setDupName(`${rule.name} Copy`);
                      const maxPriority = allItems.reduce((m, r) => Math.max(m, r.priority ?? 0), 0);
                      setDupPriority(String(maxPriority + 1));
                    }} sx={{ color: '#607D8B' }}>
                      <Copy size={16} />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title={rule._isSchedule ? 'Delete schedule' : 'Delete rule'}>
                    <IconButton size="small" onClick={(e) => { e.stopPropagation(); setDeleteTarget(rule); }}
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

      {/* Clear All Confirmation Dialog */}
      <Dialog open={showClearAll} onClose={() => setShowClearAll(false)} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ color: '#D32F2F' }}>Clear Rules & Editor</DialogTitle>
        <DialogContent>
          <DialogContentText sx={{ mb: 2 }}>
            This will reset your workspace by clearing:
          </DialogContentText>
          <Box component="ul" sx={{ m: 0, pl: 3 }}>
            <Box component="li" sx={{ mb: 0.75 }}><Typography variant="body2"><strong>Code Editor</strong> — all code in the editor will be removed</Typography></Box>
            <Box component="li" sx={{ mb: 0.75 }}><Typography variant="body2"><strong>Console Output</strong> — all logs and results will be cleared</Typography></Box>
            <Box component="li" sx={{ mb: 0.75 }}><Typography variant="body2"><strong>Business Preview</strong> — execution results will be reset</Typography></Box>
            <Box component="li"><Typography variant="body2"><strong>Rule Manager</strong> — all {totalCount} saved rule{totalCount !== 1 ? 's' : ''} and schedule{totalCount !== 1 ? 's' : ''} will be deleted</Typography></Box>
          </Box>
          <DialogContentText sx={{ mt: 2, fontWeight: 500, color: 'text.secondary', fontSize: '0.8125rem' }}>
            Event definitions and event data are preserved. This action cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowClearAll(false)} color="inherit">Cancel</Button>
          <Button
            onClick={async () => {
              setClearing(true);
              try {
                if (onClearAll) {
                  await onClearAll();
                } else {
                  await Promise.all([
                    fetch(`${API}/saved-rules`, { method: 'DELETE' }),
                    fetch(`${API}/saved-schedules`, { method: 'DELETE' }).catch(() => {}),
                  ]);
                  await loadRules();
                }
              } catch (err) {
                console.error('Clear all failed:', err);
              } finally {
                setClearing(false);
                setShowClearAll(false);
              }
            }}
            color="error" variant="contained"
            disabled={clearing}
            startIcon={clearing ? <CircularProgress size={16} color="inherit" /> : null}
          >
            {clearing ? 'Clearing…' : 'Clear Everything'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={!!deleteTarget} onClose={() => setDeleteTarget(null)}>
        <DialogTitle>Delete {deleteTarget?._isSchedule ? 'Schedule' : 'Rule'}</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Delete {deleteTarget?._isSchedule ? 'schedule' : 'rule'} "{deleteTarget?.name}"? This cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteTarget(null)} color="inherit">Cancel</Button>
          <Button onClick={() => handleDelete(deleteTarget)} color="error" variant="contained">Delete</Button>
        </DialogActions>
      </Dialog>

      {/* Duplicate Rule/Schedule Dialog */}
      <Dialog open={!!duplicateTarget} onClose={() => setDuplicateTarget(null)} maxWidth="xs" fullWidth>
        <DialogTitle>Duplicate {duplicateTarget?._isSchedule ? 'Schedule' : 'Rule'}</DialogTitle>
        <DialogContent>
          <DialogContentText sx={{ mb: 2 }}>
            Create a copy of "{duplicateTarget?.name}" with a new name and priority.
          </DialogContentText>
          <TextField
            autoFocus fullWidth size="small" label="New Name *"
            value={dupName} onChange={(e) => setDupName(e.target.value)}
            sx={{ mb: 1.5 }} />
          <TextField
            fullWidth size="small" label="Priority *" type="number"
            inputProps={{ min: 1, step: 1 }}
            value={dupPriority} onChange={(e) => setDupPriority(e.target.value)} />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDuplicateTarget(null)} color="inherit">Cancel</Button>
          <Button
            onClick={handleDuplicate}
            variant="contained"
            disabled={duplicating || !dupName.trim() || dupPriority === ''}
            startIcon={duplicating ? <CircularProgress size={14} color="inherit" /> : null}
          >
            {duplicating ? 'Duplicating…' : 'Duplicate'}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Save as Template Dialog */}
      <Dialog open={showSaveTemplate} onClose={() => setShowSaveTemplate(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Save as Template</DialogTitle>
        <DialogContent>
          <DialogContentText sx={{ mb: 2 }}>
            Create a reusable template from all {sortedRules.length} saved rule{sortedRules.length !== 1 ? 's' : ''}. This template will appear in the Accounting Templates library.
          </DialogContentText>
          <TextField
            autoFocus fullWidth size="small" label="Template Name" placeholder="e.g., Monthly Interest Accrual"
            value={templateName} onChange={(e) => setTemplateName(e.target.value)}
            sx={{ mb: 2 }}
          />
          <TextField
            fullWidth size="small" label="Description" placeholder="Describe what this template calculates..."
            value={templateDesc} onChange={(e) => setTemplateDesc(e.target.value)}
            multiline rows={2}
            sx={{ mb: 2 }}
          />
          <TextField
            fullWidth size="small" label="Category (optional)" placeholder="e.g., Loans & Lending"
            value={templateCategory} onChange={(e) => setTemplateCategory(e.target.value)}
          />
          {templateResult && (
            <Alert severity={templateResult.success ? 'success' : 'error'} sx={{ mt: 2 }}>
              {templateResult.message}
            </Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setShowSaveTemplate(false)} color="inherit">Cancel</Button>
          <Button
            disabled={!templateName.trim() || savingTemplate}
            variant="contained"
            onClick={async () => {
              setSavingTemplate(true);
              setTemplateResult(null);
              try {
                // Fetch full rules (with generatedCode) and combined code on demand
                const [fullRulesRes, codeRes] = await Promise.all([
                  fetch(`${API}/saved-rules`),
                  fetch(`${API}/combined-code`),
                ]);
                const fullRules = await fullRulesRes.json();
                const codeData = await codeRes.json();
                const combinedCode = codeData?.code || '';
                const ruleSummaries = (Array.isArray(fullRules) ? fullRules : []).map(r => ({
                  name: r.name, priority: r.priority, ruleType: r.ruleType,
                  generatedCode: r.generatedCode,
                  variables: r.variables || [],
                  conditions: r.conditions || [],
                  elseFormula: r.elseFormula || '',
                  conditionResultVar: r.conditionResultVar || 'result',
                  iterations: r.iterations || [],
                  iterConfig: r.iterConfig || {},
                  outputs: r.outputs || {},
                  customCode: r.customCode || '',
                  steps: r.steps || [],
                }));
                const res = await fetch(`${API}/user-templates`, {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({
                    name: templateName.trim(),
                    description: templateDesc.trim(),
                    category: templateCategory.trim() || 'User Created',
                    rules: ruleSummaries,
                    combinedCode,
                  }),
                });
                const data = await res.json();
                if (res.ok && data.success) {
                  // Persist the template id so future saves overwrite without showing the modal
                  try { localStorage.setItem('savedRulesTemplateId', data.id); } catch { /* ignore */ }
                  setSavedTemplateId(data.id);
                  setTemplateResult({ success: true, message: data.message || 'Template saved!' });
                  setTimeout(() => {
                    setShowSaveTemplate(false);
                    setTemplateName(''); setTemplateDesc(''); setTemplateCategory('');
                    setTemplateResult(null);
                  }, 1200);
                } else {
                  setTemplateResult({ success: false, message: data.detail || data.error || 'Save failed' });
                }
              } catch (err) {
                setTemplateResult({ success: false, message: err.message || 'Network error' });
              } finally {
                setSavingTemplate(false);
              }
            }}
          >
            {savingTemplate ? <CircularProgress size={18} /> : 'Save Template'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default SavedRules;
