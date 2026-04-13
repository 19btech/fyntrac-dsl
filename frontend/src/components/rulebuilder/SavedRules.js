import React, { useState, useEffect, useCallback } from "react";
import {
  Box, Typography, Card, CardContent, Button, IconButton, Chip,
  CircularProgress, Alert, Tooltip, Divider,
} from "@mui/material";
import { Trash2, Edit3, Calculator, GitBranch, Repeat, Database, Clock } from "lucide-react";
import { API } from "../../config";

const RULE_TYPE_META = {
  simple_calc: { label: 'Calculation', color: '#5B5FED', icon: Calculator },
  conditional: { label: 'Conditional', color: '#FF9800', icon: GitBranch },
  iteration: { label: 'Iteration', color: '#00BCD4', icon: Repeat },
  collect: { label: 'Collect', color: '#8BC34A', icon: Database },
};

const SavedRules = ({ onEditRule, refreshKey }) => {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [deleting, setDeleting] = useState(null);

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
        <Typography variant="caption" color="text.secondary">{rules.length} rule{rules.length !== 1 ? 's' : ''}</Typography>
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

      {rules.map((rule) => {
        const meta = RULE_TYPE_META[rule.ruleType] || RULE_TYPE_META.simple_calc;
        const Icon = meta.icon;
        const varCount = (rule.variables || []).filter(v => v.name).length;
        const hasTxn = rule.outputs?.createTransaction;

        return (
          <Card
            key={rule.id}
            sx={{
              mb: 1.5, cursor: 'pointer', transition: 'all 0.15s',
              border: '1px solid #E9ECEF',
              '&:hover': { borderColor: '#5B5FED', boxShadow: '0 2px 8px rgba(91,95,237,0.12)' },
            }}
            onClick={() => onEditRule(rule)}
          >
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
              <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1.5 }}>
                <Box sx={{ width: 36, height: 36, borderRadius: 1, bgcolor: `${meta.color}14`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, mt: 0.25 }}>
                  <Icon size={18} color={meta.color} />
                </Box>
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                    <Typography variant="body1" fontWeight={600} noWrap>{rule.name}</Typography>
                    <Chip size="small" label={meta.label} sx={{ fontSize: '0.6875rem', height: 20, bgcolor: `${meta.color}18`, color: meta.color, fontWeight: 600 }} />
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
