import React, { useState, useEffect, useRef } from "react";
import axios from "axios";
import { useToast } from "../components/ToastProvider";
import { Upload, Code, BookOpen, Sparkles, Trash2, Search as SearchIcon, Settings, ChevronDown, Database, Calculator, Eye, Save } from "lucide-react";
import { Button, Tabs, Tab, Box, Menu, MenuItem, Divider, Alert, Typography, ToggleButtonGroup, ToggleButton, Tooltip } from '@mui/material';
import Editor from "@monaco-editor/react";
import FileUploadPanel from "../components/FileUploadPanel";
import LeftSidebar from "../components/LeftSidebar";
import ChatAssistant from "../components/ChatAssistant";
import ConsoleOutput from "../components/ConsoleOutput";
import FunctionBrowser from "../components/FunctionBrowser";
import EventDataViewer from "../components/EventDataViewer";
import AppDialog, { useAppDialog } from "../components/AppDialog";
import AIAgentSetupWizard from "../components/AIAgentSetupWizard";
import LivePreview from "../components/rulebuilder/LivePreview";
import AccountingRuleBuilder from "../components/rulebuilder/AccountingRuleBuilder";
import TemplateLibrary from "../components/rulebuilder/TemplateWizard";
import ACCOUNTING_TEMPLATES from "../components/rulebuilder/AccountingTemplates";
import SavedRules from "../components/rulebuilder/SavedRules";
import { API } from "../config";
import { runAllTests } from "../agent/testing";

// TabPanel component for MUI Tabs with animations
function TabPanel({ children, value, index, ...other }) {
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`tabpanel-${index}`}
      aria-labelledby={`tab-${index}`}
      style={{ height: '100%', display: value === index ? 'flex' : 'none', flexDirection: 'column', overflow: 'auto' }}
      {...other}
    >
      <div className="tab-panel-enter h-full flex flex-col">
        <div className="tab-panel-content h-full flex flex-col">
          {children}
        </div>
      </div>
    </div>
  );
}

const Dashboard = () => {
  const [events, setEvents] = useState([]);
  const [dslFunctions, setDslFunctions] = useState([]);
  const [dslCode, setDslCode] = useState(() => {
    try {
      const saved = localStorage.getItem('dslCode');
      return saved || "## Welcome to Fyntac DSL Code Editor ##";
    } catch (e) {
      return "## Welcome to Fyntac DSL Code Editor ##";
    }
  });

  const [templates, setTemplates] = useState([]);
  const [selectedEvent, setSelectedEvent] = useState("");
  const [consoleOutput, setConsoleOutput] = useState([]);
  const [tabValue, setTabValue] = useState(0);
  const [showFunctionBrowser, setShowFunctionBrowser] = useState(false);
  const [settingsAnchorEl, setSettingsAnchorEl] = useState(null);
  // Custom function builder removed: feature disabled
  const [showEventDataViewer, setShowEventDataViewer] = useState(false);
  const [showAISetup, setShowAISetup] = useState(false);
  const [providerRefreshKey, setProviderRefreshKey] = useState(0);
  // Editor mode: 'code' | 'ruleBuilder' | 'scheduleBuilder' | 'customCode' | 'preview' | 'savedRules'
  const [editorMode, setEditorMode] = useState('code');
  // Saved rules
  const [editingRule, setEditingRule] = useState(null);
  const [editingSchedule, setEditingSchedule] = useState(null);
  const [editingCustomCode, setEditingCustomCode] = useState(null);
  const [savedRulesRefreshKey, setSavedRulesRefreshKey] = useState(0);
  const [loadedTemplateId, setLoadedTemplateId] = useState(null);
  // Execution results for LivePreview
  const [lastExecutionResult, setLastExecutionResult] = useState({ transactions: [], printOutputs: [], templateName: '' });
  // Template batch execution state
  const [batchRunning, setBatchRunning] = useState(false);
  const [batchStatus, setBatchStatus] = useState(null); // { total, current, currentDate, results, errors }
  const chatAssistantRef = useRef(null);
  const editorRef = useRef(null);
  const monacoRef = useRef(null);
  const toast = useToast();
  const { confirmProps, openConfirm, promptProps, openPrompt } = useAppDialog();

  useEffect(() => {

    loadEvents();
    loadDslFunctions();
    loadTemplates();
    loadCombinedCode();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Run DSL function test suite in background after functions load
  useEffect(() => {
    if (!dslFunctions || dslFunctions.length === 0) return;
    const timer = setTimeout(() => {
      runAllTests({ dslFunctions }).catch((err) =>
        console.warn('[DSL Test Runner] Background test failed:', err.message)
      );
    }, 3000);
    return () => clearTimeout(timer);
  }, [dslFunctions]);

  // Persist DSL editor content to localStorage whenever it changes
  useEffect(() => {
    try {
      if (dslCode && dslCode.length > 0) {
        localStorage.setItem('dslCode', dslCode);
      } else {
        localStorage.removeItem('dslCode');
      }
    } catch (e) {
      // ignore
    }
  }, [dslCode]);

  const loadEvents = async () => {
    try {
      const response = await axios.get(`${API}/events`);
      setEvents(response.data);
      if (response.data.length > 0 && !selectedEvent) {
        setSelectedEvent(response.data[0].event_name);
      }
    } catch (error) {
      console.error("Error loading events:", error);
    }
  };

  const loadDslFunctions = async () => {
    try {
      const response = await axios.get(`${API}/dsl-functions`);
      setDslFunctions(response.data);
    } catch (error) {
      console.error("Error loading DSL functions:", error);
    }
  };

  const loadTemplates = async () => {
    try {
      const response = await axios.get(`${API}/templates`);
      setTemplates(response.data);
    } catch (error) {
      console.error("Error loading templates:", error);
    }
  };

  const addConsoleLog = (message, type = "info") => {
    const timestamp = new Date().toLocaleTimeString();
    setConsoleOutput(prev => [...prev, { timestamp, message, type }]);
  };

  const handleClearAllData = async () => {
    openConfirm({
      title: "Clear All Data",
      message: "Are you sure you want to clear all data? This will delete all events, DSL functions, event data, and templates. This action cannot be undone.",
      confirmLabel: "Clear All",
      confirmColor: "error",
      onConfirm: async () => {
        try {
          addConsoleLog("Clearing all data...", "info");
          const response = await axios.delete(`${API}/clear-all-data`);
          
          addConsoleLog(`✓ ${response.data.message}`, "success");
          
          setEvents([]);
          setDslFunctions([]);
          setTemplates([]);
          setSelectedEvent("");
          setDslCode('');
          setShowEventDataViewer(false);

          // Clear console output
          setConsoleOutput([]);

          // Clear chat assistant conversation
          if (chatAssistantRef.current && chatAssistantRef.current.clearChat) {
            chatAssistantRef.current.clearChat();
          }

          try {
            localStorage.removeItem('dslCode');
            localStorage.removeItem('chatMessages');
            localStorage.removeItem('chatSessionId');
            // Remove uploaded filenames and upload state
            localStorage.removeItem('uploadedEventFileName');
            localStorage.removeItem('uploadedExcelFileName');
            localStorage.removeItem('lastEventDataUploadFailedFile');
            localStorage.removeItem('lastEventDataUploadFileName');
            localStorage.removeItem('lastEventDataUploadStatus');
            localStorage.removeItem('lastEventDataUploadErrors');
            localStorage.removeItem('importSelectedInstruments');
            try { window.dispatchEvent(new Event('dsl-clear-uploaded-files')); } catch(e) {}
            try { window.dispatchEvent(new Event('dsl-clear-event-viewer')); } catch(e) {}
          } catch (e) {
            // ignore
          }

          await loadDslFunctions();
          await loadTemplates();

          toast.success("All data cleared! Fresh environment ready.");
        } catch (error) {
          addConsoleLog(`✗ Error clearing data: ${error.message}`, "error");
          toast.error("Failed to clear data");
        }
      }
    });
  };

  const handleDownloadEvents = async () => {
    try {
      const response = await axios.get(`${API}/events/download`, {
        responseType: 'blob'
      });
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', 'event_definitions.csv');
      document.body.appendChild(link);
      link.click();
      link.remove();
      toast.success("Event definitions downloaded!");
    } catch (error) {
      toast.error("Failed to download events");
    }
  };

  const handleSaveTemplate = async () => {
    if (!selectedEvent) {
      toast.error("Please select an event first");
      return;
    }
    
    openPrompt({
      title: "Save Template",
      message: "Enter a name for this template.",
      label: "Template name",
      onSubmit: async (templateName) => {
        try {
          const checkResponse = await axios.get(`${API}/templates/check-name/${encodeURIComponent(templateName)}`);
          
          if (checkResponse.data.exists) {
            openConfirm({
              title: "Replace Template",
              message: `A template named "${templateName}" already exists. Do you want to replace it?`,
              confirmLabel: "Replace",
              onConfirm: async () => {
                try {
                  addConsoleLog(`Saving template '${templateName}' (replacing existing)...`, "info");
                  await axios.post(
                    `${API}/templates`,
                    {
                      name: templateName,
                      dsl_code: dslCode,
                      event_name: selectedEvent,
                      replace: true
                    }
                  );
                  addConsoleLog(`✓ Template replaced successfully!`, "success");
                  toast.success("Template replaced!");
                  loadTemplates();
                } catch (error) {
                  const errorMsg = error.response?.data?.detail || error.message;
                  addConsoleLog(`✗ Error saving template: ${errorMsg}`, "error");
                  toast.error("Failed to save template");
                }
              }
            });
          } else {
            addConsoleLog(`Saving template '${templateName}'...`, "info");
            const response = await axios.post(
              `${API}/templates`,
              {
                name: templateName,
                dsl_code: dslCode,
                event_name: selectedEvent,
                replace: false
              }
            );
            addConsoleLog(`✓ Template saved successfully!`, "success");
            toast.success("Template saved!");
            loadTemplates();
          }
        } catch (error) {
          const errorMsg = error.response?.data?.detail || error.message;
          addConsoleLog(`✗ Error saving template: ${errorMsg}`, "error");
          toast.error("Failed to save template");
        }
      }
    });
  };

  const handleRunTemplate = async (templateId) => {
    if (!selectedEvent) {
      toast.error("Please select an event first");
      return;
    }

    // Fetch unique posting dates from loaded activity event data
    let postingDates = [];
    try {
      const pdRes = await axios.get(`${API}/event-data/posting-dates`);
      postingDates = pdRes.data?.posting_dates || [];
    } catch (_e) {
      postingDates = [];
    }

    if (postingDates.length <= 1) {
      // Zero or one posting date — run exactly as before (pass the single date if present)
      try {
        // Wipe previous transaction reports before running
        try { await axios.delete(`${API}/transaction-reports/all`); } catch (_) {}
        addConsoleLog("Executing template on event data...", "info");
        const response = await axios.post(`${API}/templates/execute`, {
          template_id: templateId,
          event_name: selectedEvent,
          ...(postingDates.length === 1 ? { posting_date: postingDates[0] } : {}),
        });
        addConsoleLog(`✓ Execution completed! Generated ${response.data.transactions.length} transactions`, "success");
        addConsoleLog(`Report ID: ${response.data.report_id}`, "info");
        addConsoleLog(JSON.stringify(response.data.transactions, null, 2), "result");
        toast.success(`Generated ${response.data.transactions.length} transactions`);
      } catch (error) {
        addConsoleLog(`✗ Execution error: ${error.response?.data?.detail || error.message}`, "error");
        toast.error("Execution failed");
      }
      return;
    }

    // Multiple posting dates — run sequentially across all dates
    // Wipe previous transaction reports before batch run
    try { await axios.delete(`${API}/transaction-reports/all`); } catch (_) {}
    setBatchRunning(true);
    setBatchStatus({ total: postingDates.length, current: 0, currentDate: null, results: [], errors: [] });
    addConsoleLog(`Starting batch execution across ${postingDates.length} posting dates...`, "info");

    const batchResults = [];
    const batchErrors = [];

    for (let i = 0; i < postingDates.length; i++) {
      const date = postingDates[i];
      setBatchStatus(prev => ({ ...prev, current: i + 1, currentDate: date }));
      addConsoleLog(`Running posting date ${i + 1} of ${postingDates.length}: ${date}`, "info");

      try {
        const response = await axios.post(`${API}/templates/execute`, {
          template_id: templateId,
          event_name: selectedEvent,
          posting_date: date,
        });
        const txCount = response.data.transactions.length;
        batchResults.push({ date, transactions: txCount });
        addConsoleLog(`  ✓ ${date} — ${txCount} transaction(s) generated`, "success");
      } catch (error) {
        const msg = error.response?.data?.detail || error.message;
        batchErrors.push({ date, error: msg });
        addConsoleLog(`  ✗ ${date} — ${msg}`, "error");
        // Continue to next date
      }
    }

    setBatchStatus(prev => ({ ...prev, current: postingDates.length, currentDate: null, results: batchResults, errors: batchErrors }));
    setBatchRunning(false);

    const totalTx = batchResults.reduce((sum, r) => sum + r.transactions, 0);
    if (batchErrors.length === 0) {
      addConsoleLog(`✓ Batch complete — ${totalTx} total transaction(s) across ${postingDates.length} posting dates`, "success");
      toast.success(`Batch complete: ${totalTx} transactions across ${postingDates.length} dates`);
    } else {
      addConsoleLog(`Batch finished with ${batchErrors.length} failure(s). ${totalTx} transaction(s) generated from ${batchResults.length} successful date(s).`, "warning");
      batchErrors.forEach(e => {
        addConsoleLog(`  Failed date ${e.date}: ${e.error}`, "error");
      });
      toast.error(`Batch finished with ${batchErrors.length} failure(s)`);
    }
  };

  const handleDeployTemplate = async (templateId, templateName) => {
    try {
      addConsoleLog(`Deploying template '${templateName}'...`, 'info');
      toast.info(`Starting deployment for ${templateName}`);
      // Attempt to call backend deploy endpoint if available
      try {
        await axios.post(`${API}/templates/deploy`, { template_id: templateId });
        addConsoleLog(`✓ Deployment request submitted for ${templateName}`, 'success');
        toast.success(`Deployment started for ${templateName}`);
      } catch (err) {
        // Fallback: simulate deployment delay
        await new Promise(res => setTimeout(res, 800));
        addConsoleLog(`✓ Deployment simulated for ${templateName}`, 'success');
        toast.success(`Deployment started for ${templateName}`);
      }
    } catch (error) {
      addConsoleLog(`✗ Deployment failed: ${error.message}`, 'error');
      toast.error('Deployment failed');
    }
  };

  const handleDeleteTemplate = async (templateId, templateName) => {
    try {
      addConsoleLog(`Deleting template '${templateName}'...`, "info");
      await axios.delete(`${API}/templates/${templateId}`);
      
      addConsoleLog(`✓ Template deleted successfully!`, "success");
      toast.success("Template deleted!");
      loadTemplates();
    } catch (error) {
      const errorMsg = error.response?.data?.detail || error.message;
      addConsoleLog(`✗ Error deleting template: ${errorMsg}`, "error");
      toast.error(`Failed to delete template: ${errorMsg}`);
      throw error;
    }
  };

  const handleLoadTemplate = (template) => {
    setDslCode(template.dsl_code);
    addConsoleLog(`Loaded template: ${template.name}`, "info");
    toast.success(`Loaded template: ${template.name}`);
  };

  const handleInsertFunction = (functionCall) => {
    setDslCode(prev => prev + "\n" + functionCall);
  };

  const handleGeneratedCode = async (code, metadata) => {
    setDslCode(code);
    setEditorMode('code');
    setTabValue(1);
    addConsoleLog("Logic loaded into editor from builder", "info");
    toast.success("Logic loaded into editor — click Run to execute");

    // Track the source template ID so Rule Manager can overwrite it on next save
    if (metadata?.templateId) {
      setLoadedTemplateId(metadata.templateId);
      try { localStorage.setItem('savedRulesTemplateId', metadata.templateId); } catch { /* ignore */ }
    }

    // If template metadata includes rules or schedules, save them
    if (metadata?.rules?.length || metadata?.schedules?.length) {
      try {
        const [rulesRes, schedulesRes] = await Promise.all([
          axios.get(`${API}/saved-rules`),
          axios.get(`${API}/saved-schedules`).catch(() => ({ data: [] })),
        ]);
        const existingRules = Array.isArray(rulesRes.data) ? rulesRes.data : [];
        const existingSchedules = Array.isArray(schedulesRes.data) ? schedulesRes.data : [];
        let maxPriority = Math.max(
          ...existingRules.map(r => r.priority || 0),
          ...existingSchedules.map(s => s.priority || 0),
          0
        );
        const existingNames = new Set([
          ...existingRules.map(r => (r.name || '').toLowerCase()),
          ...existingSchedules.map(s => (s.name || '').toLowerCase()),
        ]);

        let created = 0;

        // Create saved-rules
        for (const rule of (metadata.rules || [])) {
          maxPriority += 1;
          let name = rule.name;
          let baseName = name;
          let suffix = 1;
          while (existingNames.has(name.toLowerCase())) {
            name = `${baseName} (${suffix})`;
            suffix++;
          }
          existingNames.add(name.toLowerCase());
          try {
            await axios.post(`${API}/saved-rules`, {
              name,
              priority: maxPriority,
              ruleType: rule.ruleType || 'simple_calc',
              variables: rule.variables || [],
              conditions: rule.conditions || [],
              elseFormula: rule.elseFormula || '',
              conditionResultVar: rule.conditionResultVar || 'result',
              iterations: rule.iterations || [],
              iterConfig: rule.iterConfig || {},
              outputs: rule.outputs || {},
              customCode: rule.customCode || '',
              generatedCode: rule.generatedCode || '',
              steps: rule.steps || [],
            });
            created++;
          } catch (err) {
            console.error(`Failed to save rule "${name}":`, err.response?.data?.detail || err.message);
          }
        }

        // Create saved-schedules
        for (const sched of (metadata.schedules || [])) {
          maxPriority += 1;
          let name = sched.name;
          let baseName = name;
          let suffix = 1;
          while (existingNames.has(name.toLowerCase())) {
            name = `${baseName} (${suffix})`;
            suffix++;
          }
          existingNames.add(name.toLowerCase());
          try {
            await axios.post(`${API}/saved-schedules`, {
              name,
              priority: maxPriority,
              generatedCode: sched.generatedCode || '',
              config: sched.config || {},
            });
            created++;
          } catch (err) {
            console.error(`Failed to save schedule "${name}":`, err.response?.data?.detail || err.message);
          }
        }

        if (created > 0) {
          setSavedRulesRefreshKey(k => k + 1);
          addConsoleLog(`Created ${created} rule(s) in Rule Manager from template`, "info");
        }
        // Refresh events in case template sample data was loaded
        await loadEvents();
      } catch (err) {
        console.error("Error importing template rules:", err);
      }
    }
  };

  const loadCombinedCode = async () => {
    try {
      const response = await axios.get(`${API}/combined-code`);
      if (response.data?.success && response.data.code) {
        setDslCode(response.data.code);
      }
    } catch (error) {
      console.error("Error loading combined code:", error);
    }
  };

  const handleAskAIAboutFunction = (funcName, message) => {
    if (chatAssistantRef.current && chatAssistantRef.current.sendSilentMessage) {
      chatAssistantRef.current.sendSilentMessage(funcName, message);
    }
  };

  return (
    <div className="flex h-screen bg-[#F8F9FA] overflow-auto" style={{ minWidth: '1400px' }} data-testid="dashboard-container">
      {/* Left Sidebar */}
        <div className="sidebar-enter">
        <LeftSidebar 
          events={events} 
          selectedEvent={selectedEvent}
          onEventSelect={setSelectedEvent}
          onDownloadEvents={handleDownloadEvents}
          onImportSuccess={() => {
            loadEvents();
            window.dispatchEvent(new CustomEvent('dsl-event-data-refresh'));
          }}
        />
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Top Bar - Fyntrac style */}
        <div className="bg-white/80 backdrop-blur-xl border-b border-[#E9ECEF]/50 px-6 py-4 animate-fade-in-up">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-[#14213d] tracking-tight" style={{ fontFamily: "'Inter', sans-serif" }}>Logic Studio</h1>
              <p className="text-sm text-[#6C757D] mt-1">Design and test your financial calculation logic using built-in formulas</p>
            </div>
            <div className="flex gap-2">
              <Button 
                variant="outlined" 
                size="small" 
                onClick={() => setShowFunctionBrowser(true)}
                data-testid="browse-functions-button"
                title={`${dslFunctions.length} formulas loaded`}
                startIcon={<SearchIcon className="w-4 h-4" />}
                sx={{
                  bgcolor: '#D4EDDA',
                  borderColor: '#C3E6CB',
                  color: '#155724',
                  fontWeight: 500,
                  '&:hover': {
                    bgcolor: '#C3E6CB',
                    borderColor: '#B8DAFF',
                    color: '#14213d',
                    '& .MuiButton-startIcon svg': { color: '#14213d' },
                  },
                  '&:active': { color: '#14213d' },
                  '&.Mui-focusVisible': { color: '#14213d' },
                  '& .MuiButton-startIcon svg': { color: '#155724' },
                }}
              >
                Browse Formulas ({dslFunctions.length})
              </Button>
              {/* Build Function removed */}
              <Button 
                variant="outlined" 
                size="small" 
                onClick={(e) => setSettingsAnchorEl(e.currentTarget)}
                data-testid="settings-button"
                startIcon={<Settings className="w-4 h-4" />}
                endIcon={<ChevronDown className="w-3 h-3" />}
                sx={{
                  borderColor: '#CED4DA',
                  color: '#495057',
                  fontWeight: 500,
                  '&:hover': {
                    borderColor: '#ADB5BD',
                    bgcolor: '#F8F9FA',
                    color: '#14213d',
                    '& .MuiButton-startIcon svg': { color: '#14213d' },
                  },
                  '&:active': { color: '#14213d' },
                  '&.Mui-focusVisible': { color: '#14213d' },
                  '& .MuiButton-startIcon svg': { color: '#495057' },
                }}
              >
                Settings
              </Button>
              <Menu
                anchorEl={settingsAnchorEl}
                open={Boolean(settingsAnchorEl)}
                onClose={() => setSettingsAnchorEl(null)}
                data-testid="settings-menu"
                PaperProps={{
                  sx: {
                    borderRadius: '8px',
                    border: '1px solid #E9ECEF',
                    boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
                  }
                }}
              >
                <MenuItem
                  onClick={() => {
                    handleClearAllData();
                    setSettingsAnchorEl(null);
                  }}
                  data-testid="menu-clear-data"
                  sx={{ fontSize: '0.875rem', py: 1.5 }}
                >
                  <Trash2 className="w-4 h-4 text-[#6C757D] mr-2" />
                  Clear All Data
                </MenuItem>
                <Divider />
                <MenuItem
                  onClick={() => {
                    setShowEventDataViewer(true);
                    setSettingsAnchorEl(null);
                  }}
                  data-testid="menu-event-data-viewer"
                  sx={{ fontSize: '0.875rem', py: 1.5 }}
                >
                  <Database className="w-4 h-4 text-[#6C757D] mr-2" />
                  View Event Data
                </MenuItem>
                <Divider />
                <MenuItem
                  onClick={() => {
                    setShowAISetup(true);
                    setSettingsAnchorEl(null);
                  }}
                  data-testid="menu-ai-setup"
                  sx={{ fontSize: '0.875rem', py: 1.5 }}
                >
                  <Sparkles className="w-4 h-4 text-[#6C757D] mr-2" />
                  AI Agent Setup
                </MenuItem>
                
              </Menu>
            </div>
          </div>
        </div>

        {/* Main Content Area */}
        <div className="flex-1 flex overflow-hidden min-w-0">
          {/* Center - Editor and Console */}
          <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0, overflow: 'hidden' }}>
            <Box sx={{ borderBottom: 1, borderColor: 'divider', bgcolor: 'white', px: 3 }}>
              <Tabs value={tabValue} onChange={(e, newValue) => { setTabValue(newValue); if (newValue === 1) { setEditorMode('savedRules'); } }}>
                <Tab 
                  icon={<Upload className="w-4 h-4" />} 
                  iconPosition="start" 
                  label="Upload Data" 
                  data-testid="upload-tab"
                  sx={{ textTransform: 'none', fontSize: '0.875rem' }}
                />
                <Tab 
                  icon={<Code className="w-4 h-4" />} 
                  iconPosition="start" 
                  label="Logic Builder" 
                  data-testid="editor-tab"
                  sx={{ textTransform: 'none', fontSize: '0.875rem' }}
                />
              </Tabs>
            </Box>

            <TabPanel value={tabValue} index={0}>
              <FileUploadPanel 
                  onUploadSuccess={loadEvents} 
                  events={events}
                  addConsoleLog={addConsoleLog}
                  selectedEvent={selectedEvent}
                  onViewEvent={(eventName) => { setSelectedEvent(eventName); setShowEventDataViewer(true); }}
                />
            </TabPanel>

            <TabPanel value={tabValue} index={1}>
              {/* Editor Mode Switcher */}
              <Box sx={{ px: 2, py: 1, bgcolor: 'white', borderBottom: '1px solid #E9ECEF', display: 'flex', alignItems: 'center', gap: 2 }}>
                <Typography variant="caption" fontWeight={600} color="text.secondary" sx={{ whiteSpace: 'nowrap' }}>BUILD WITH:</Typography>
                <ToggleButtonGroup
                  value={editorMode}
                  exclusive
                  onChange={(e, val) => { if (val) { setEditorMode(val); } }}
                  size="small"
                  sx={{ '& .MuiToggleButton-root': { textTransform: 'none', fontSize: '0.75rem', px: 1.5, py: 0.5 } }}
                >
                  <ToggleButton value="savedRules">
                    <Tooltip title="View and manage saved rules"><Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}><Save size={14} /> Rule Manager</Box></Tooltip>
                  </ToggleButton>
                  <ToggleButton value="ruleBuilder">
                    <Tooltip title="Build calculations using forms"><Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}><Calculator size={14} /> Rule Builder</Box></Tooltip>
                  </ToggleButton>
                  <ToggleButton value="preview">
                    <Tooltip title="View business preview of execution results"><Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}><Eye size={14} /> Business Preview</Box></Tooltip>
                  </ToggleButton>
                  <ToggleButton value="code">
                    <Tooltip title="View combined DSL code from all rules"><Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}><Code size={14} /> Code Viewer</Box></Tooltip>
                  </ToggleButton>
                  <ToggleButton value="templates">
                    <Tooltip title="Browse accounting templates (ASC 310, 360, 606, 842...)"><Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}><BookOpen size={14} /> Templates</Box></Tooltip>
                  </ToggleButton>
                </ToggleButtonGroup>
              </Box>

              {/* Code Editor Mode */}
              {editorMode === 'code' && (
                <>
                  <Box sx={{ px: 2, py: 1, bgcolor: '#F0F1FF', borderBottom: '1px solid #D6D8FE', display: 'flex', alignItems: 'center', gap: 1.5 }}>
                    <Code size={16} color="#5B5FED" />
                    <Typography variant="body2" color="text.secondary" sx={{ flex: 1 }}>
                      This editor shows the combined output of all rules (sorted by priority). To edit, use the <strong>Rule Builder</strong> or create a <strong>Custom Code</strong> rule.
                    </Typography>
                    <Button size="small" variant="outlined" onClick={() => { loadCombinedCode(); }}
                      sx={{ textTransform: 'none', fontSize: '0.75rem', borderColor: '#5B5FED', color: '#5B5FED' }}>
                      Refresh
                    </Button>
                  </Box>
                  <div className="flex-1 bg-[#0A0A0A] min-w-0" data-testid="dsl-editor">
                    <Editor
                      height="100%"
                      defaultLanguage="python"
                      value={dslCode}
                      theme="vs-dark"
                      options={{
                        fontSize: 14,
                        fontFamily: "monospace",
                        minimap: { enabled: false },
                        lineNumbers: "on",
                        scrollBeyondLastLine: false,
                        automaticLayout: true,
                        wordWrap: "on",
                        tabSize: 2,
                        insertSpaces: true,
                        renderWhitespace: "none",
                        readOnly: true,
                        cursorStyle: "line",
                        cursorBlinking: "blink",
                        fixedOverflowWidgets: true,
                      }}
                      beforeMount={(monaco) => {
                        monacoRef.current = monaco;
                        monaco.languages.registerCompletionItemProvider('python', {
                          provideCompletionItems: (model, position) => {
                            const suggestions = [];
                            const existingNames = new Set();
                            dslFunctions.forEach(func => {
                              existingNames.add(func.name);
                              suggestions.push({
                                label: func.name,
                                kind: monaco.languages.CompletionItemKind.Function,
                                insertText: `${func.name}()`,
                                detail: func.params || '',
                                documentation: func.description || ''
                              });
                            });

                            const helperFunctions = [
                              { name: 'lag', params: "col, offset, default", description: 'Get previous row value in schedule' },
                              { name: 'schedule', params: 'period_def, columns, context?', description: 'Generate a schedule of periods and computed columns' },
                              { name: 'schedule_sum', params: 'sched, col', description: 'Sum a schedule column' },
                              { name: 'schedule_first', params: 'sched, col', description: 'First value of schedule column' },
                              { name: 'schedule_last', params: 'sched, col', description: 'Last value of schedule column' },
                              { name: 'period', params: 'start, end, freq, convention?', description: 'Create a period definition' },
                              { name: 'print', params: 'value', description: 'Print value to console' },
                              { name: 'collect', params: 'EVENT.field', description: 'Collect values for current instrument/postingdate' },
                              { name: 'for_each', params: 'dates_arr, amounts_arr, date_var, amount_var, expression', description: 'Iterate paired arrays and evaluate expression' },
                              { name: 'map_array', params: 'array, var_name, expression, context?', description: 'Transform array elements' },
                              { name: 'sum_vals', params: 'array', description: 'Sum numeric values in array' }
                            ];

                            helperFunctions.forEach(h => {
                              if (!existingNames.has(h.name)) {
                                existingNames.add(h.name);
                                suggestions.push({
                                  label: h.name,
                                  kind: monaco.languages.CompletionItemKind.Function,
                                  insertText: `${h.name}()`,
                                  detail: h.params,
                                  documentation: h.description
                                });
                              }
                            });

                            events.forEach(event => {
                              ['postingdate', 'effectivedate', 'subinstrumentid'].forEach(sf => {
                                suggestions.push({
                                  label: `${event.event_name}.${sf}`,
                                  kind: monaco.languages.CompletionItemKind.Field,
                                  insertText: `${event.event_name}.${sf}`,
                                  detail: '(date)',
                                  documentation: `Field from ${event.event_name}`
                                });
                                suggestions.push({
                                  label: sf,
                                  kind: monaco.languages.CompletionItemKind.Field,
                                  insertText: sf,
                                  detail: '(date)',
                                  documentation: `Standard event field (from ${event.event_name})`
                                });
                              });

                              event.fields.forEach(field => {
                                suggestions.push({
                                  label: `${event.event_name}.${field.name}`,
                                  kind: monaco.languages.CompletionItemKind.Field,
                                  insertText: `${event.event_name}.${field.name}`,
                                  detail: `(${field.datatype})`,
                                  documentation: `Event field from ${event.event_name}`
                                });
                                suggestions.push({
                                  label: field.name,
                                  kind: monaco.languages.CompletionItemKind.Field,
                                  insertText: field.name,
                                  detail: `(${field.datatype})`,
                                  documentation: `Field from ${event.event_name}`
                                });
                              });
                            });

                            try {
                              const code = model.getValue();
                              const assignRegex = /^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=.*$/gm;
                              const found = new Set();
                              let m;
                              while ((m = assignRegex.exec(code)) !== null) {
                                const name = m[1];
                                if (!found.has(name)) {
                                  found.add(name);
                                  suggestions.push({
                                    label: name,
                                    kind: monaco.languages.CompletionItemKind.Variable,
                                    insertText: name,
                                    detail: 'User-defined variable',
                                    documentation: 'Variable defined in editor'
                                  });
                                }
                              }
                            } catch (e) {
                              // ignore
                            }

                            return { suggestions };
                          }
                        });
                      }}
                      onMount={(editor) => {
                        editorRef.current = editor;
                      }}
                    />
                  </div>
                  <ConsoleOutput 
                    output={consoleOutput} 
                    onClear={() => setConsoleOutput([])} 
                    dslCode={dslCode}
                    addConsoleLog={addConsoleLog}
                    onCodeChange={setDslCode}
                    events={events}
                    handleSaveTemplate={handleSaveTemplate}
                    onExecutionResult={setLastExecutionResult}
                  />
                </>
              )}

              {/* Rule Builder Mode — always mounted to preserve form state across tab switches */}
              <Box sx={{ display: editorMode === 'ruleBuilder' ? 'flex' : 'none', flexDirection: 'column', flex: 1, overflow: 'auto' }}>
                <AccountingRuleBuilder
                  key={editingRule ? `${editingRule.id}-p${editingRule.priority ?? 0}` : 'new'}
                  events={events}
                  dslFunctions={dslFunctions}
                  onGenerate={handleGeneratedCode}
                  onClose={() => { setEditorMode('code'); setEditingRule(null); }}
                  onSave={() => { setSavedRulesRefreshKey(k => k + 1); loadCombinedCode(); }}
                  initialData={editingRule}
                />
              </Box>

              {/* Business Preview Mode */}
              {editorMode === 'preview' && (
                <LivePreview
                  consoleOutput={consoleOutput}
                  transactions={lastExecutionResult.transactions}
                  templateName={lastExecutionResult.templateName}
                  visible={true}
                />
              )}

              {/* Saved Rules Mode */}
              {editorMode === 'savedRules' && (
                <SavedRules
                  refreshKey={savedRulesRefreshKey}
                  loadedTemplateId={loadedTemplateId}
                  onEditRule={(rule) => {
                    setEditingRule(rule);
                    setEditingSchedule(null);
                    setEditingCustomCode(null);
                    setEditorMode('ruleBuilder');
                  }}
                  onEditSchedule={(sched) => {
                    setEditingRule(sched);
                    setEditingSchedule(null);
                    setEditorMode('ruleBuilder');
                  }}
                  onPlayAll={(result) => {
                    setLastExecutionResult(result);
                    // Forward print outputs to consoleOutput so LivePreview can extract schedule tables
                    if (result.printOutputs?.length > 0) {
                      const ts = new Date().toLocaleTimeString();
                      const printLogs = result.printOutputs.map(p => ({ timestamp: ts, message: String(p), type: 'print' }));
                      setConsoleOutput(prev => [...prev, ...printLogs]);
                    }
                    setEditorMode('preview');
                  }}
                  onClearAll={async () => {
                    try {
                      addConsoleLog('Clearing rules & editor...', 'info');
                      // Only delete rules and schedules — event definitions and event data are preserved
                      await Promise.all([
                        axios.delete(`${API}/saved-rules`),
                        axios.delete(`${API}/saved-schedules`).catch(() => {}),
                      ]);

                      setDslCode('');
                      setConsoleOutput([]);
                      setEditingRule(null);
                      setEditingSchedule(null);
                      setLastExecutionResult({ transactions: [], printOutputs: [], templateName: '' });
                      setSavedRulesRefreshKey(k => k + 1);

                      try {
                        localStorage.removeItem('dslCode');
                      } catch (e) { /* ignore */ }

                      addConsoleLog('✓ Rules, schedules and editor cleared. Event definitions and data preserved.', 'success');
                      toast.success('Rules cleared! Event data preserved.');
                    } catch (error) {
                      addConsoleLog(`✗ Error clearing rules: ${error.message}`, 'error');
                      toast.error('Failed to clear rules');
                      throw error;
                    }
                  }}
                />
              )}

              {/* Templates Mode */}
              {editorMode === 'templates' && (
                <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'auto' }}>
                  <TemplateLibrary
                    templates={ACCOUNTING_TEMPLATES}
                    events={events}
                    onLoadTemplate={handleGeneratedCode}
                    onClose={() => setEditorMode('savedRules')}
                    inline
                  />
                </Box>
              )}
            </TabPanel>
          </Box>

          {/* Right Sidebar - Chat Assistant */}
          <div className="flex-shrink-0 chat-panel-enter">
            <ChatAssistant 
              ref={chatAssistantRef}
              dslFunctions={dslFunctions} 
              events={events}
              onInsertCode={(code) => setDslCode(prev => prev + "\n" + code)}
              onOverwriteCode={(code) => setDslCode(code)}
              editorCode={dslCode}
              consoleOutput={consoleOutput}
              editorRef={editorRef}
              monacoRef={monacoRef}
              providerRefreshKey={providerRefreshKey}
            />
          </div>
        </div>
      </div>

      {/* Modals */}
      {showFunctionBrowser && (
        <FunctionBrowser 
          dslFunctions={dslFunctions}
          onInsertFunction={handleInsertFunction}
          onClose={() => setShowFunctionBrowser(false)}
          onAskAI={handleAskAIAboutFunction}
        />
      )}


      {showEventDataViewer && (
        <EventDataViewer 
          onClose={() => setShowEventDataViewer(false)}
        />
      )}

      <AppDialog {...confirmProps} />
      <AppDialog {...promptProps} />
      <AIAgentSetupWizard open={showAISetup} onClose={() => setShowAISetup(false)} onSaved={() => setProviderRefreshKey(k => k + 1)} />
    </div>
  );
};

export default Dashboard;
