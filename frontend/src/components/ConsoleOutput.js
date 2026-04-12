import React, { useState } from "react";
import axios from "axios";
import { Button, Box } from '@mui/material';
import AppDialog, { useAppDialog } from './AppDialog';
import { Trash2, Terminal, Play, Table2, Code, Wand2, Download, Save, X } from "lucide-react";
import { useToast } from "./ToastProvider";
import { API } from '../config';
import { formatErrorForConsole } from '../agent/testing/translateError';
import PostingDateModal from './PostingDateModal';

// Helper to check if a string is JSON array/object
const tryParseJSON = (str) => {
  try {
    const parsed = JSON.parse(str);
    return parsed;
  } catch {
    return null;
  }
};

// Helper to check if data is tabular (array of objects)
const isTabularData = (data) => {
  if (!Array.isArray(data)) return false;
  if (data.length === 0) return false;
  return data.every(item => typeof item === 'object' && item !== null && !Array.isArray(item));
};

// Helper to check if data is a simple array of numbers
const isSimpleArray = (data) => {
  if (!Array.isArray(data)) return false;
  return data.every(item => typeof item === 'number' || typeof item === 'string');
};

// Table component for schedule/array data
const DataTable = ({ data, title }) => {
  if (!data || data.length === 0) return null;
  
  // Get all unique keys from all objects
  const allKeys = [...new Set(data.flatMap(obj => Object.keys(obj)))];
  
  // Format cell value for display
  const formatValue = (val) => {
    if (val === null || val === undefined) return '-';
    if (typeof val === 'number') {
      // Format numbers with appropriate precision
      if (Number.isInteger(val)) return val.toLocaleString();
      return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
    }
    if (typeof val === 'boolean') return val ? 'true' : 'false';
    return String(val);
  };

  return (
    <div className="my-2 rounded overflow-hidden border border-slate-700 w-full">
      {title && (
        <div className="bg-slate-700 px-2 py-1 text-xs text-slate-300 flex items-center gap-1">
          <Table2 className="w-3 h-3" />
          {title} ({data.length} rows)
        </div>
      )}
      <div className="block w-full overflow-x-auto">
        <table className="inline-table min-w-max text-xs">
          <thead>
            <tr className="bg-slate-800">
              <th className="px-2 py-1 text-left text-slate-400 font-medium border-b border-slate-700">#</th>
              {allKeys.map((key, i) => (
                <th 
                  key={i} 
                  className="px-2 py-1 text-left text-slate-400 font-medium border-b border-slate-700"
                >
                  {key}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, rowIdx) => (
              <tr 
                key={rowIdx} 
                className={rowIdx % 2 === 0 ? 'bg-slate-900' : 'bg-slate-850'}
              >
                <td className="px-2 py-1 text-slate-500 border-b border-slate-800">{rowIdx + 1}</td>
                {allKeys.map((key, colIdx) => (
                  <td 
                    key={colIdx} 
                    className={`px-2 py-1 border-b border-slate-800 whitespace-nowrap ${
                      typeof row[key] === 'number' ? 'text-cyan-400 text-right' : 'text-slate-300'
                    }`}>
                    {formatValue(row[key])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// Simple array display component
const SimpleArrayDisplay = ({ data, title }) => {
  if (!data || data.length === 0) return null;
  
  const formatValue = (val) => {
    if (typeof val === 'number') {
      if (Number.isInteger(val)) return val.toLocaleString();
      return val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 });
    }
    return String(val);
  };

  return (
    <div className="my-2 rounded overflow-hidden border border-slate-700">
      {title && (
        <div className="bg-slate-700 px-2 py-1 text-xs text-slate-300 flex items-center gap-1">
          <Code className="w-3 h-3" />
          {title} ({data.length} values)
        </div>
      )}
      <div className="bg-slate-900 p-2 overflow-x-auto">
        <div className="flex flex-wrap gap-1">
          {data.map((val, idx) => (
            <span 
              key={idx} 
              className="px-2 py-0.5 bg-slate-800 rounded text-cyan-400 text-xs font-mono"
            >
              {formatValue(val)}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
};

// Period object display
const PeriodDisplay = ({ data }) => {
  if (!data || data.type !== 'period') return null;
  
  return (
    <div className="my-2 rounded overflow-hidden border border-slate-700">
      <div className="bg-slate-700 px-2 py-1 text-xs text-slate-300 flex items-center gap-1">
        <Table2 className="w-3 h-3" />
        Period Definition
      </div>
      <div className="bg-slate-900 p-2 text-xs">
        <div className="grid grid-cols-4 gap-2 mb-2">
          <div>
            <span className="text-slate-500">Start:</span>
            <span className="ml-1 text-green-400">{data.start}</span>
          </div>
          <div>
            <span className="text-slate-500">End:</span>
            <span className="ml-1 text-green-400">{data.end}</span>
          </div>
          <div>
            <span className="text-slate-500">Frequency:</span>
            <span className="ml-1 text-amber-400">{data.freq}</span>
          </div>
          <div>
            <span className="text-slate-500">Convention:</span>
            <span className="ml-1 text-purple-400">{data.convention}</span>
          </div>
        </div>
        {data.dates && data.dates.length > 0 && (
          <div>
            <span className="text-slate-500">Dates ({data.dates.length}):</span>
            <div className="flex flex-wrap gap-1 mt-1">
              {data.dates.map((date, idx) => (
                <span key={idx} className="px-1.5 py-0.5 bg-slate-800 rounded text-cyan-400 text-xs">
                  {date}
                </span>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

// Smart print output renderer
const PrintOutputRenderer = ({ output }) => {
  const parsed = tryParseJSON(output);

  if (parsed !== null) {
    // Period object
    if (parsed.type === 'period') {
      return <PeriodDisplay data={parsed} />;
    }

    // If it's an array, handle multi-schedule shapes explicitly
    if (Array.isArray(parsed)) {
      // Case A: array of generate_schedules result objects { item_name?, schedule: [...] }
      if (parsed.every(x => x && typeof x === 'object' && 'schedule' in x)) {
        return (
          <>
            {parsed.map((res, i) => {
              const sched = Array.isArray(res.schedule) ? res.schedule : [];
              const name = res.item_name || res.item_name === 0 ? res.item_name : (res.item_index ? `Item ${res.item_index}` : `Schedule ${i + 1}`);
              const subKeys = ['subinstrument_id', 'subinstrumentid', 'subInstrumentId', 'subInstrumentID', 'subinstrumentId', 'subInstrument', 'subinstrument'];
              let sub = null;
              for (const k of subKeys) {
                if (k in res && res[k] != null) { sub = res[k]; break; }
              }
              const title = sub ? `${name} (subInstrumentId: ${sub}) Schedule` : `${name} Schedule`;
              return <DataTable key={i} data={sched} title={title} />;
            })}
          </>
        );
      }

      // Case B: array of schedule arrays -> render each as its own table
      if (parsed.every(item => Array.isArray(item) && item.length > 0 && typeof item[0] === 'object')) {
        return (
          <>
            {parsed.map((sched, i) => (
              <DataTable key={i} data={sched} title={`Schedule ${i + 1}`} />
            ))}
          </>
        );
      }

      // Fall through: if it's an array of objects (single combined table), render as table
      if (isTabularData(parsed)) {
        return <DataTable data={parsed} title="Schedule" />;
      }

      // Simple numeric/string arrays
      if (isSimpleArray(parsed)) {
        return <SimpleArrayDisplay data={parsed} title="Array" />;
      }
    }

    // For other objects, show formatted JSON
    return (
      <pre className="text-yellow-300 bg-slate-800 p-2 rounded text-xs overflow-x-auto my-1 whitespace-pre-wrap">
        {JSON.stringify(parsed, null, 2)}
      </pre>
    );
  }
  
  // Plain text output
  return <span className="text-yellow-300">{output}</span>;
};

const ConsoleOutput = ({ output, onClear, dslCode, addConsoleLog, onCodeChange, events, handleSaveTemplate }) => {
  const [running, setRunning] = useState(false);
  const [postingDateModalOpen, setPostingDateModalOpen] = useState(false);
  const [availablePostingDates, setAvailablePostingDates] = useState([]);
  const toast = useToast();

  // Import all event fields as variables into the editor
  const handleImportInputs = () => {
    if (!events || events.length === 0) {
      toast.error("No events available to import");
      return;
    }

    const lines = [];
    lines.push("## ═══════════════════════════════════════════════════════════════");
    lines.push("## IMPORTED EVENT FIELDS");
    lines.push("## ═══════════════════════════════════════════════════════════════");
    lines.push("## Hierarchy: postingDate → instrumentId → subInstrumentId → effectiveDates");
    lines.push("");
    events.forEach((event) => {
      const eventName = event.event_name;
      const etype = (event.eventType || event.event_type || '').toString().toLowerCase();

      if (etype === 'reference') {
        // Minimal import for reference events: do not expose standard hierarchy variables
        lines.push(`## ─── ${eventName} (reference event) ───`);
        lines.push(`# Note: reference events do not expose posting/effective/instrument/subinstrument fields`);
        // Import user-defined fields so DSL can reference them (unconditional)
        event.fields.forEach((field) => {
          lines.push(`${eventName}_${field.name} = ${eventName}.${field.name}`);
        });
        // Provide collected arrays using collect_all for reference events
        event.fields.forEach((field) => {
          if (field.datatype === 'decimal') {
            lines.push(`${eventName}_${field.name}_arr = collect_all(${eventName}.${field.name})`);
          }
        });
        lines.push("");
        return;
      }

      // Section 1: Single-Value Fields (for simple calculations)
      lines.push(`## ─── ${eventName} Single Values (merged/latest row) ───`);
      lines.push(`${eventName}_postingdate = ${eventName}.postingdate`);
      lines.push(`${eventName}_effectivedate = ${eventName}.effectivedate`);
      lines.push(`${eventName}_subinstrumentid = ${eventName}.subinstrumentid`);
      
      event.fields.forEach((field) => {
        lines.push(`${eventName}_${field.name} = ${eventName}.${field.name}`);
      });
      
      lines.push("");
      
      // Section 2: Collected Arrays (for multi-row scenarios)
      lines.push(`## ─── ${eventName} Collected Arrays (all rows for instrument) ───`);
      lines.push(`${eventName}_effectivedates_arr = collect_by_instrument(${eventName}.effectivedate)`);
      lines.push(`${eventName}_subinstrumentids_arr = collect_subinstrumentids()`);
      
      // Only add array collection for decimal fields (most common for iteration)
      event.fields.forEach((field) => {
        if (field.datatype === 'decimal') {
          lines.push(`${eventName}_${field.name}_arr = collect_by_instrument(${eventName}.${field.name})`);
        }
      });
      
      lines.push("");
    });

    // Do not append trailing header/tips; leave only imported variables

    const importedCode = lines.join('\n');
    
    // Prepend to existing code or set as new code
    const newCode = dslCode ? importedCode + dslCode : importedCode;
    
    if (onCodeChange) {
      onCodeChange(newCode);
      toast.success(`Imported ${events.length} event(s) with single values & arrays`);
      addConsoleLog(`✓ Imported fields from ${events.length} event(s) (single values + collected arrays)`, "success");
    }
  };

  // Beautify DSL code
  const handleBeautify = () => {
    if (!dslCode || !dslCode.trim()) {
      toast.error("No code to beautify");
      return;
    }

    const lines = dslCode.split('\n');
    const beautifiedLines = [];
    let indentLevel = 0;
    const indentStr = '    '; // 4 spaces

    for (let line of lines) {
      // Trim the line
      let trimmed = line.trim();
      
      if (!trimmed) {
        beautifiedLines.push('');
        continue;
      }

      // Check for closing braces/brackets to decrease indent before adding line
      const closingOnly = /^[\}\]\)]+[,]?$/.test(trimmed);
      if (closingOnly || trimmed.startsWith('}') || trimmed.startsWith(']') || trimmed.startsWith(')')) {
        indentLevel = Math.max(0, indentLevel - 1);
      }

      // Add spacing around operators (but be careful with strings and comments)
      if (!trimmed.startsWith('//') && !trimmed.startsWith('#')) {
        // Add space around = but not == or !=
        trimmed = trimmed.replace(/([^=!<>])=([^=])/g, '$1 = $2');
        // Clean up multiple spaces
        trimmed = trimmed.replace(/  +/g, ' ');
        // Add space after commas
        trimmed = trimmed.replace(/,([^\s])/g, ', $1');
        // Add space around comparison operators
        trimmed = trimmed.replace(/([<>])([^=])/g, '$1 $2');
        trimmed = trimmed.replace(/([^<>=!])([<>])/g, '$1 $2');
      }

      // Add the indented line
      beautifiedLines.push(indentStr.repeat(indentLevel) + trimmed);

      // Check for opening braces/brackets to increase indent after adding line
      const openCount = (trimmed.match(/[\{\[\(]/g) || []).length;
      const closeCount = (trimmed.match(/[\}\]\)]/g) || []).length;
      indentLevel += (openCount - closeCount);
      indentLevel = Math.max(0, indentLevel);
    }

    const beautified = beautifiedLines.join('\n');
    
    if (onCodeChange) {
      onCodeChange(beautified);
      toast.success("Code beautified!");
      addConsoleLog("✓ Code formatted", "success");
    }
  };

  const { confirmProps, openConfirm } = useAppDialog();

  // Clear the editor content with confirmation
  const handleClearEditor = () => {
    openConfirm({
      title: "Clear Editor",
      message: "This action will clear all content from the editor. Do you want to proceed?",
      confirmLabel: "Clear",
      confirmColor: "error",
      onConfirm: () => {
        if (onCodeChange) {
          onCodeChange("");
          addConsoleLog("✓ Editor cleared", "success");
          toast.success("Editor cleared");
        }
      }
    });
  };

  const handleRunCode = async () => {
    if (!dslCode || !dslCode.trim()) {
      toast.error("No DSL code to run");
      return;
    // Fetch unique posting dates from loaded activity event data.
    // If there is more than one, show the date-selection modal before running.
    try {
      const pdRes = await axios.get(`${API}/event-data/posting-dates`);
      const dates = pdRes.data?.posting_dates || [];
      if (dates.length > 1) {
        setAvailablePostingDates(dates);
        setPostingDateModalOpen(true);
        return; // execution continues in handlePostingDateConfirm
      }
      // Zero or one posting date — proceed normally (pass the single date if present)
      await executeCode(dates.length === 1 ? dates[0] : null);
    } catch (_e) {
      // If the endpoint fails (e.g. no event data loaded), just run without date scope
      await executeCode(null);
    }
  };

  const handlePostingDateConfirm = async (selectedDate) => {
    setPostingDateModalOpen(false);
    await executeCode(selectedDate);
  };

  const executeCode = async (postingDate) => {
    setRunning(true);
    addConsoleLog("Running DSL code...", "info");

    try {
      const payload = { dsl_code: dslCode };
      if (postingDate) payload.posting_date = postingDate;

      const response = await axios.post(`${API}/dsl/run`, payloadonst response = await axios.post(`${API}/dsl/run`, {
        dsl_code: dslCode
      });

      if (response.data.success) {
        const eventsUsed = response.data.events_used?.length > 0 
          ? response.data.events_used.join(', ') 
          : 'standalone mode';
        addConsoleLog(`✓ Executed successfully (${eventsUsed})`, "success");
        
        // Display print outputs first (debugging output)
        if (response.data.print_outputs && response.data.print_outputs.length > 0) {
          addConsoleLog("── Print Output ──", "info");
          response.data.print_outputs.forEach((output) => {
            addConsoleLog(output, "print");
          });
          addConsoleLog("──────────────────", "info");
        }
        
        addConsoleLog(`✓ Generated ${response.data.transactions.length} transaction(s)`, "success");
        
        // Display transactions in console
        response.data.transactions.forEach((txn, idx) => {
          addConsoleLog(
            `[${idx + 1}] ${txn.instrumentid} | ${txn.subinstrumentid || '1'} | ${txn.transactiontype} | ${txn.amount.toFixed(2)} | ${txn.postingdate}`,
            "result"
          );
        });
      } else {
        addConsoleLog(`✗ ${formatErrorForConsole(response.data.error)}`, "error");
      }
    } catch (error) {
      addConsoleLog(`✗ ${formatErrorForConsole(error.response?.data?.detail || error.message)}`, "error");
    } finally {
      setRunning(false);
    }
  };

  const getTypeColor = (type) => {
    switch (type) {
      case "success": return "text-emerald-400";
      case "error": return "text-red-400";
      case "result": return "text-cyan-400";
      case "warning": return "text-amber-400";
      case "print": return "text-yellow-300";
      default: return "text-slate-400";
    }
  };

  const renderLogMessage = (log) => {
    // For print type, use the smart renderer
    if (log.type === "print") {
      return <PrintOutputRenderer output={log.message} />;
    }
    // For other types, just show the message
    return <span className={getTypeColor(log.type)}>{log.message}</span>;
  };

  return (
    <div className="h-80 w-full max-w-full bg-[#0D1117] border-t border-[#30363D] min-w-0 overflow-hidden" data-testid="console-output">
      <div className="flex items-center justify-between px-4 py-1 bg-[#161B22] border-b border-[#30363D]">
        <div className="flex items-center gap-2">
          <Terminal className="w-3 h-3 text-[#8B949E]" />
          <span className="text-sm font-semibold text-[#E6EDF3]" style={{ fontFamily: 'Inter' }}>Console</span>
        </div>
        
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Button
              variant="outlined"
              size="small"
              onClick={handleClearEditor}
              startIcon={<X size={12} />}
              data-testid="clear-editor-button"
              sx={{
                fontSize: '0.75rem',
                minHeight: 28,
                px: 1,
                py: 0.25,
                color: '#8B949E',
                borderColor: '#30363D',
                bgcolor: 'transparent',
                '&:hover': {
                  borderColor: '#8B949E',
                  bgcolor: 'rgba(139, 148, 158, 0.1)',
                  color: '#E6EDF3',
                },
              }}
            >
              Clear Editor
            </Button>

          <Button 
            variant="outlined" 
            size="small" 
            onClick={handleImportInputs}
            disabled={!events || events.length === 0}
            startIcon={<Download size={12} />}
            data-testid="import-inputs-button"
            sx={{
              fontSize: '0.75rem',
              minHeight: 28,
              px: 1,
              py: 0.25,
              color: '#8B949E',
              borderColor: '#30363D',
              bgcolor: 'transparent',
              '&:hover': {
                borderColor: '#8B949E',
                bgcolor: 'rgba(139, 148, 158, 0.1)',
                color: '#E6EDF3',
              },
              '&:disabled': {
                color: '#484F58',
                borderColor: '#21262D',
              },
            }}
          >
            Import Inputs
          </Button>
          <Button 
            variant="outlined" 
            size="small" 
            onClick={handleSaveTemplate}
            startIcon={<Save size={12} />}
            data-testid="save-template-button"
            sx={{
              fontSize: '0.75rem',
              minHeight: 28,
              px: 1,
              py: 0.25,
              color: '#8B949E',
              borderColor: '#30363D',
              bgcolor: 'transparent',
              '&:hover': {
                borderColor: '#8B949E',
                bgcolor: 'rgba(139, 148, 158, 0.1)',
                color: '#E6EDF3',
              },
            }}
          >
            Save as Template
          </Button>
          <Button 
            variant="outlined" 
            size="small" 
            onClick={handleBeautify}
            startIcon={<Wand2 size={12} />}
            data-testid="beautify-code-button"
            sx={{
              fontSize: '0.75rem',
              minHeight: 28,
              px: 1,
              py: 0.25,
              color: '#8B949E',
              borderColor: '#30363D',
              bgcolor: 'transparent',
              '&:hover': {
                borderColor: '#8B949E',
                bgcolor: 'rgba(139, 148, 158, 0.1)',
                color: '#E6EDF3',
              },
            }}
          >
            Beautify
          </Button>
          <Button 
            variant="contained" 
            size="small" 
            onClick={handleRunCode}
            disabled={running}
            startIcon={<Play size={12} />}
            data-testid="run-dsl-button"
            sx={{
              fontSize: '0.8125rem',
              fontWeight: 600,
              minHeight: 28,
              px: 1.25,
              py: 0.25,
              bgcolor: '#14213D',
              color: '#FFFFFF',
              border: 'none',
              boxShadow: '0 2px 8px rgba(20, 33, 61, 0.3)',
              '&:hover': {
                bgcolor: '#1D3557',
                boxShadow: '0 4px 12px rgba(20, 33, 61, 0.4)',
              },
              '&:disabled': {
                bgcolor: '#21262D',
                color: '#484F58',
              },
            }}
          >
            {running ? "Running..." : "Run"}
          </Button>
          <Button 
            variant="text" 
            size="small" 
            onClick={onClear}
            data-testid="clear-console-button"
            sx={{
              minWidth: 'auto',
              px: 1,
              py: 0.25,
              color: '#8B949E',
              '&:hover': {
                color: '#F85149',
                bgcolor: 'rgba(248, 81, 73, 0.1)',
              },
            }}
          >
            <Trash2 size={14} />
          </Button>
      <PostingDateModal
        open={postingDateModalOpen}
        postingDates={availablePostingDates}
        onConfirm={handlePostingDateConfirm}
        onCancel={() => setPostingDateModalOpen(false)}
      />
        </Box>
      </div>
      
      <Box sx={{ height: 'calc(100% - 52px)', p: 2, maxWidth: '100%', width: '100%', overflow: 'auto' }}>
        <div className="space-y-1 font-mono text-xs">
          {output.length === 0 ? (
            <div className="text-[#484F58]">Console output will appear here... Click "Run" to execute DSL code</div>
          ) : (
            output.map((log, idx) => (
              <div key={idx} data-testid={`console-log-${idx}`}>
                {log.type !== "print" && (
                  <div className="flex gap-2">
                    <span className="text-[#484F58]">[{log.timestamp}]</span>
                    {renderLogMessage(log)}
                  </div>
                )}
                {log.type === "print" && (
                  <div>
                    {renderLogMessage(log)}
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      </Box>
      <AppDialog {...confirmProps} />
    </div>
  );
};

export default ConsoleOutput;
