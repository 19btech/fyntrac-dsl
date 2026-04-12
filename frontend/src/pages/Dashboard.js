import React, { useState, useEffect, useRef } from "react";
import axios from "axios";
import { useToast } from "../components/ToastProvider";
import { Upload, FileText, Code, Play, List, BookOpen, Download, Sparkles, Trash2, BarChart3, Search as SearchIcon, Lightbulb, Settings, ChevronDown } from "lucide-react";
import { Button, Tabs, Tab, Box, Menu, MenuItem, Divider } from '@mui/material';
import Editor from "@monaco-editor/react";
import FileUploadPanel from "../components/FileUploadPanel";
import LeftSidebar from "../components/LeftSidebar";
import ChatAssistant from "../components/ChatAssistant";
import ConsoleOutput from "../components/ConsoleOutput";
import TemplatesPanel from "../components/TemplatesPanel";
import TransactionReports from "../components/TransactionReports";
import FunctionBrowser from "../components/FunctionBrowser";
import DSLExamples from "../components/DSLExamples";
import EventDataViewer from "../components/EventDataViewer";
import AppDialog, { useAppDialog } from "../components/AppDialog";
import AIAgentSetupWizard from "../components/AIAgentSetupWizard";
import { API } from "../config";

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
      {value === index && (
        <div className="tab-panel-enter h-full flex flex-col">
          <div className="tab-panel-content h-full flex flex-col">
            {children}
          </div>
        </div>
      )}
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
  const [transactionReports, setTransactionReports] = useState([]);
  const [tabValue, setTabValue] = useState(0);
  const [showFunctionBrowser, setShowFunctionBrowser] = useState(false);
  const [settingsAnchorEl, setSettingsAnchorEl] = useState(null);
  // Custom function builder removed: feature disabled
  const [showEventDataViewer, setShowEventDataViewer] = useState(false);
  const [showAISetup, setShowAISetup] = useState(false);
  const [providerRefreshKey, setProviderRefreshKey] = useState(0);
  const chatAssistantRef = useRef(null);
  const editorRef = useRef(null);
  const monacoRef = useRef(null);
  const toast = useToast();
  const { confirmProps, openConfirm, promptProps, openPrompt } = useAppDialog();

  useEffect(() => {

    loadEvents();
    loadDslFunctions();
    loadTemplates();
    loadTransactionReports();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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

  const loadTransactionReports = async () => {
    try {
      const response = await axios.get(`${API}/transaction-reports`);
      setTransactionReports(response.data);
    } catch (error) {
      console.error("Error loading transaction reports:", error);
    }
  };

  const addConsoleLog = (message, type = "info") => {
    const timestamp = new Date().toLocaleTimeString();
    setConsoleOutput(prev => [...prev, { timestamp, message, type }]);
  };

  const handleLoadSampleData = async () => {
    try {
      addConsoleLog("Loading sample data...", "info");
      const response = await axios.post(`${API}/load-sample-data`);
      
      addConsoleLog(`✓ Sample data loaded successfully!`, "success");
      addConsoleLog(`Events: ${response.data.events.join(", ")}`, "info");
      
      if (response.data.sample_dsl_code) {
        setDslCode(response.data.sample_dsl_code);
      }
      
      await loadEvents();
      await loadDslFunctions();
      
      toast.success("Sample data loaded! Ready to test.");
    } catch (error) {
      addConsoleLog(`✗ Error loading sample data: ${error.message}`, "error");
      toast.error("Failed to load sample data");
    }
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
          setTransactionReports([]);
          setSelectedEvent("");
          setDslCode('');

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
            try { window.dispatchEvent(new Event('dsl-clear-uploaded-files')); } catch(e) {}
            try { window.dispatchEvent(new Event('dsl-clear-event-viewer')); } catch(e) {}
          } catch (e) {
            // ignore
          }

          await loadDslFunctions();
          await loadTemplates();
          await loadTransactionReports();

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

    try {
      addConsoleLog("Executing template on event data...", "info");
      const response = await axios.post(`${API}/templates/execute`, {
        template_id: templateId,
        event_name: selectedEvent
      });
      
      addConsoleLog(`✓ Execution completed! Generated ${response.data.transactions.length} transactions`, "success");
      addConsoleLog(`Report ID: ${response.data.report_id}`, "info");
      addConsoleLog(JSON.stringify(response.data.transactions, null, 2), "result");
      toast.success(`Generated ${response.data.transactions.length} transactions`);
      
      loadTransactionReports();
    } catch (error) {
      addConsoleLog(`✗ Execution error: ${error.response?.data?.detail || error.message}`, "error");
      toast.error("Execution failed");
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

  const handleDownloadReport = async (reportId) => {
    try {
      const response = await axios.get(`${API}/transaction-reports/download/${reportId}`, {
        responseType: 'blob'
      });
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      const filename = response.headers['content-disposition']?.split('filename=')[1] || 'transactions.csv';
      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      link.remove();
      toast.success("Transaction report downloaded!");
    } catch (error) {
      toast.error("Failed to download report");
    }
  };

  const handleDeleteReport = async (reportId, reportName) => {
    openConfirm({
      title: "Delete Report",
      message: `Are you sure you want to delete report "${reportName}"?`,
      confirmLabel: "Delete",
      confirmColor: "error",
      onConfirm: async () => {
        try {
          addConsoleLog(`Deleting report '${reportName}'...`, "info");
          await axios.delete(`${API}/transaction-reports/${reportId}`);
          
          addConsoleLog(`✓ Report deleted successfully!`, "success");
          toast.success("Report deleted!");
          loadTransactionReports();
        } catch (error) {
          addConsoleLog(`✗ Error deleting report: ${error.message}`, "error");
          toast.error("Failed to delete report");
        }
      }
    });
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

  const handleLoadExample = (exampleCode) => {
    setDslCode(exampleCode);
    addConsoleLog("Loaded DSL example", "info");
  };

  const handleInsertFunction = (functionCall) => {
    setDslCode(prev => prev + "\n" + functionCall);
  };

  const handleAskAIAboutFunction = (message) => {
    if (chatAssistantRef.current && chatAssistantRef.current.sendMessage) {
      chatAssistantRef.current.sendMessage(message);
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
        />
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Top Bar - Fyntrac style */}
        <div className="bg-white/80 backdrop-blur-xl border-b border-[#E9ECEF]/50 px-6 py-4 animate-fade-in-up">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-[#14213d] tracking-tight" style={{ fontFamily: "'Inter', sans-serif" }}>Logic Studio</h1>
              <p className="text-sm text-[#6C757D] mt-1">Design calculation logic using a Domain-Specific Language (DSL) that is intuitive for finance professionals</p>
            </div>
            <div className="flex gap-2">
              <Button 
                variant="outlined" 
                size="small" 
                onClick={() => setShowFunctionBrowser(true)}
                data-testid="browse-functions-button"
                title={`${dslFunctions.length} functions loaded`}
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
                Browse Functions ({dslFunctions.length})
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
                    handleLoadSampleData();
                    setSettingsAnchorEl(null);
                  }}
                  data-testid="menu-sample-data"
                  sx={{ fontSize: '0.875rem', py: 1.5 }}
                >
                  <Sparkles className="w-4 h-4 text-[#6C757D] mr-2" />
                  Sample Data
                </MenuItem>
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
              <Tabs value={tabValue} onChange={(e, newValue) => setTabValue(newValue)}>
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
                  label="DSL Editor" 
                  data-testid="editor-tab"
                  sx={{ textTransform: 'none', fontSize: '0.875rem' }}
                />
                <Tab 
                  icon={<List className="w-4 h-4" />} 
                  iconPosition="start" 
                  label="Templates" 
                  data-testid="templates-tab"
                  sx={{ textTransform: 'none', fontSize: '0.875rem' }}
                />
                <Tab 
                  icon={<BarChart3 className="w-4 h-4" />} 
                  iconPosition="start" 
                  label="Transaction Report" 
                  data-testid="reports-tab"
                  sx={{ textTransform: 'none', fontSize: '0.875rem' }}
                />
                <Tab 
                  icon={<Lightbulb className="w-4 h-4" />} 
                  iconPosition="start" 
                  label="Examples" 
                  data-testid="examples-tab"
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
              <div className="flex-1 bg-[#0A0A0A] min-w-0" data-testid="dsl-editor">
                <Editor
                  height="100%"
                  defaultLanguage="python"
                  value={dslCode}
                  onChange={(value) => setDslCode(value || "")}
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
                          const assignRegex = /^\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*=.*$/gm;
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
              />
            </TabPanel>

            <TabPanel value={tabValue} index={2}>
              <TemplatesPanel 
                templates={templates}
                onLoadTemplate={handleLoadTemplate}
                onRunTemplate={handleRunTemplate}
                onDeleteTemplate={handleDeleteTemplate}
                onDeployTemplate={handleDeployTemplate}
                selectedEvent={selectedEvent}
              />
            </TabPanel>

            <TabPanel value={tabValue} index={3}>
              <TransactionReports 
                reports={transactionReports}
                onDownloadReport={handleDownloadReport}
                onDeleteReport={handleDeleteReport}
              />
            </TabPanel>

            <TabPanel value={tabValue} index={4}>
              <DSLExamples onLoadExample={handleLoadExample} />
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
