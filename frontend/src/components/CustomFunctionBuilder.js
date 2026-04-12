import React, { useState, useEffect } from "react";
import axios from "axios";
import { useToast } from "./ToastProvider";
import { Plus, Trash2, Save, X, Code, Brackets, FileCode, Info, ChevronDown, ChevronUp, Edit2, HelpCircle, BookOpen, Lightbulb } from "lucide-react";
import { Button, Card, CardContent, TextField, Box, Chip, Tooltip, IconButton, Dialog, DialogTitle, DialogContent } from '@mui/material';
import AppDialog, { useAppDialog } from './AppDialog';

const API = '/api';

const CATEGORIES = [
  "Interest Calculations",
  "Present/Future Value",
  "Amortization",
  "Date Functions",
  "Risk & Volatility",
  "Portfolio",
  "Bonds",
  "Options",
  "Taxes",
  "Currency",
  "Utilities",
  "Custom"
];

const RETURN_TYPES = ["decimal", "string", "date", "boolean"];

// Documentation Guide Component
const DocumentationGuide = ({ onClose }) => (
  <div className="absolute inset-0 bg-white z-10 flex flex-col rounded-r-xl">
    <div className="p-4 border-b border-slate-200 flex items-center justify-between bg-gradient-to-r from-blue-50 to-cyan-50">
      <div className="flex items-center gap-2">
        <BookOpen className="w-5 h-5 text-blue-600" />
        <h3 className="font-semibold text-blue-800">How to Create Functions</h3>
      </div>
      <Button variant="ghost" size="sm" onClick={onClose}>
        <X className="w-4 h-4" />
      </Button>
    </div>
    
    <ScrollArea className="flex-1 p-4">
      <div className="space-y-6 text-sm">
        {/* Quick Start */}
        <section>
          <h4 className="font-semibold text-slate-800 flex items-center gap-2 mb-2">
            <Lightbulb className="w-4 h-4 text-amber-500" />
            Quick Start
          </h4>
          <div className="bg-slate-50 p-3 rounded-lg space-y-2">
            <p className="text-slate-600">1. Click <strong>"New Function"</strong> to start</p>
            <p className="text-slate-600">2. Give your function a unique <strong>name</strong></p>
            <p className="text-slate-600">3. Define <strong>parameters</strong> (inputs)</p>
            <p className="text-slate-600">4. Write the <strong>formula/logic</strong></p>
            <p className="text-slate-600">5. Click <strong>Save</strong></p>
          </div>
        </section>

        {/* Naming Convention */}
        <section>
          <h4 className="font-semibold text-slate-800 mb-2">📝 Naming Convention</h4>
          <div className="bg-green-50 p-3 rounded-lg">
            <p className="text-slate-600 mb-2">Use <code className="bg-green-100 px-1 rounded">snake_case</code> (lowercase with underscores)</p>
            <div className="space-y-1 text-xs">
              <p className="text-green-700">✓ <code>calculate_interest</code></p>
              <p className="text-green-700">✓ <code>my_custom_pv</code></p>
              <p className="text-red-600">✗ <code>CalculateInterest</code></p>
              <p className="text-red-600">✗ <code>my-function</code></p>
            </div>
          </div>
        </section>

        {/* Parameters */}
        <section>
          <h4 className="font-semibold text-slate-800 mb-2">📥 Parameters</h4>
          <div className="bg-blue-50 p-3 rounded-lg space-y-2">
            <p className="text-slate-600">Parameters are the inputs your function receives.</p>
            <div className="text-xs space-y-1">
              <p><strong>Types:</strong></p>
              <p className="text-slate-600">• <code className="bg-blue-100 px-1 rounded">decimal</code> - Numbers (e.g., 1000.50)</p>
              <p className="text-slate-600">• <code className="bg-blue-100 px-1 rounded">string</code> - Text (e.g., "USD")</p>
              <p className="text-slate-600">• <code className="bg-blue-100 px-1 rounded">date</code> - Dates (e.g., "2024-01-01")</p>
              <p className="text-slate-600">• <code className="bg-blue-100 px-1 rounded">boolean</code> - True/False</p>
            </div>
          </div>
        </section>

        {/* Formula Writing */}
        <section>
          <h4 className="font-semibold text-slate-800 mb-2">⚡ Writing Formulas</h4>
          <div className="bg-purple-50 p-3 rounded-lg space-y-3">
            <p className="text-slate-600">Use parameter names directly in your formula.</p>
            
            <div>
              <p className="text-xs font-medium text-purple-700 mb-1">Simple Interest Example:</p>
              <code className="block bg-slate-900 text-green-400 p-2 rounded text-xs">
                return principal * rate * time
              </code>
            </div>
            
            <div>
              <p className="text-xs font-medium text-purple-700 mb-1">Compound Interest Example:</p>
              <code className="block bg-slate-900 text-green-400 p-2 rounded text-xs">
                return principal * (1 + rate) ** periods
              </code>
            </div>
            
            <div>
              <p className="text-xs font-medium text-purple-700 mb-1">With Conditional Logic (use iif):</p>
              <code className="block bg-slate-900 text-green-400 p-2 rounded text-xs whitespace-pre">{`return iif(amount > 0, amount * rate, 0)`}</code>
            </div>
          </div>
        </section>

        {/* Conditional Logic */}
        <section>
          <h4 className="font-semibold text-slate-800 mb-2">🔀 Conditional Logic</h4>
          <div className="bg-indigo-50 p-3 rounded-lg space-y-2">
            <p className="text-slate-600 text-xs">Use <code className="bg-indigo-100 px-1 rounded">iif()</code> for conditional logic (NOT if):</p>
            <code className="block bg-slate-900 text-green-400 p-2 rounded text-xs">
              iif(condition, value_if_true, value_if_false)
            </code>
            <div className="text-xs space-y-1 mt-2">
              <p className="text-indigo-700">Examples:</p>
              <p className="text-slate-600">• <code className="bg-indigo-100 px-1 rounded">iif(amount &gt; 1000, "Large", "Small")</code></p>
              <p className="text-slate-600">• <code className="bg-indigo-100 px-1 rounded">iif(rate == 0, 0, principal * rate)</code></p>
              <p className="text-slate-600">• <code className="bg-indigo-100 px-1 rounded">iif(balance &gt; 0, balance * 0.01, 0)</code></p>
            </div>
            <p className="text-red-600 text-xs mt-2">⚠️ Use == for equality (not =)</p>
          </div>
        </section>

        {/* Available Operators */}
        <section>
          <h4 className="font-semibold text-slate-800 mb-2">🔧 Available Operators</h4>
          <div className="bg-orange-50 p-3 rounded-lg">
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div>
                <p className="font-medium text-orange-700">Arithmetic:</p>
                <p className="text-slate-600">+ - * / ** (power)</p>
              </div>
              <div>
                <p className="font-medium text-orange-700">Comparison:</p>
                <p className="text-slate-600">== != &gt; &lt; &gt;= &lt;=</p>
              </div>
              <div>
                <p className="font-medium text-orange-700">Logical:</p>
                <p className="text-slate-600">and, or, not</p>
              </div>
              <div>
                <p className="font-medium text-orange-700">Built-in:</p>
                <p className="text-slate-600">abs(), min(), max(), round()</p>
              </div>
            </div>
          </div>
        </section>

        {/* Using Other DSL Functions */}
        <section>
          <h4 className="font-semibold text-slate-800 mb-2">🔗 Using Other DSL Functions</h4>
          <div className="bg-cyan-50 p-3 rounded-lg space-y-2">
            <p className="text-slate-600 text-xs">You can call any built-in DSL function in your formula:</p>
            <code className="block bg-slate-900 text-green-400 p-2 rounded text-xs">
              return npv(rate, cashflows) + pv(rate, periods, payment)
            </code>
          </div>
        </section>

        {/* Complete Example */}
        <section>
          <h4 className="font-semibold text-slate-800 mb-2">📋 Complete Example</h4>
          <div className="bg-slate-100 p-3 rounded-lg space-y-2 text-xs">
            <div className="grid grid-cols-2 gap-2">
              <div>
                <p className="font-medium text-slate-700">Name:</p>
                <code className="text-purple-600">loan_payment</code>
              </div>
              <div>
                <p className="font-medium text-slate-700">Category:</p>
                <code className="text-purple-600">Amortization</code>
              </div>
            </div>
            <div>
              <p className="font-medium text-slate-700">Description:</p>
              <p className="text-slate-600">Calculate monthly loan payment</p>
            </div>
            <div>
              <p className="font-medium text-slate-700">Parameters:</p>
              <p className="text-slate-600">principal (decimal), rate (decimal), months (decimal)</p>
            </div>
            <div>
              <p className="font-medium text-slate-700">Formula:</p>
              <code className="block bg-slate-900 text-green-400 p-2 rounded mt-1">
                monthly_rate = rate / 12{"\n"}
                return principal * monthly_rate / (1 - (1 + monthly_rate) ** -months)
              </code>
            </div>
            <div>
              <p className="font-medium text-slate-700">Example:</p>
              <code className="text-blue-600">loan_payment(100000, 0.05, 360)</code>
            </div>
          </div>
        </section>
      </div>
    </ScrollArea>
  </div>
);

const CustomFunctionBuilder = ({ onClose, onFunctionSaved }) => {
  const toast = useToast();
  const [customFunctions, setCustomFunctions] = useState([]);
  const [showBuilder, setShowBuilder] = useState(false);
  const [showDocumentation, setShowDocumentation] = useState(false);
  const [expandedFunction, setExpandedFunction] = useState(null);
  const [loading, setLoading] = useState(false);
  const [editingFunction, setEditingFunction] = useState(null); // Track if editing
  
  // New function form state
  const [newFunction, setNewFunction] = useState({
    name: "",
    category: "Custom",
    description: "",
    parameters: [{ name: "", type: "decimal", description: "" }],
    returnType: "decimal",
    formula: "",
    example: ""
  });

  useEffect(() => {
    loadCustomFunctions();
  }, []);

  const loadCustomFunctions = async () => {
    try {
      const response = await axios.get(`${API}/custom-functions`);
      setCustomFunctions(response.data);
    } catch (error) {
      console.error("Error loading custom functions:", error);
    }
  };

  const resetForm = () => {
    setNewFunction({
      name: "",
      category: "Custom",
      description: "",
      parameters: [{ name: "", type: "decimal", description: "" }],
      returnType: "decimal",
      formula: "",
      example: ""
    });
    setEditingFunction(null);
  };

  const handleAddParameter = () => {
    setNewFunction(prev => ({
      ...prev,
      parameters: [...prev.parameters, { name: "", type: "decimal", description: "" }]
    }));
  };

  const handleRemoveParameter = (index) => {
    setNewFunction(prev => ({
      ...prev,
      parameters: prev.parameters.filter((_, i) => i !== index)
    }));
  };

  const handleParameterChange = (index, field, value) => {
    setNewFunction(prev => ({
      ...prev,
      parameters: prev.parameters.map((p, i) => 
        i === index ? { ...p, [field]: value } : p
      )
    }));
  };

  const handleEditFunction = (func) => {
    // Populate form with existing function data
    setNewFunction({
      name: func.name,
      category: func.category,
      description: func.description,
      parameters: func.parameters || [{ name: "", type: "decimal", description: "" }],
      returnType: func.return_type || "decimal",
      formula: func.formula,
      example: func.example || ""
    });
    setEditingFunction(func.id);
    setShowBuilder(true);
  };

  const handleSaveFunction = async () => {
    // Validation
    if (!newFunction.name.trim()) {
      toast.error("Function name is required");
      return;
    }
    if (!newFunction.description.trim()) {
      toast.error("Description is required");
      return;
    }
    if (!newFunction.formula.trim()) {
      toast.error("Formula/logic is required");
      return;
    }
    if (newFunction.parameters.some(p => !p.name.trim())) {
      toast.error("All parameters must have names");
      return;
    }

    // Check for duplicate function names (only for new functions)
    if (!editingFunction) {
      const existingNames = customFunctions.map(f => f.name.toLowerCase());
      if (existingNames.includes(newFunction.name.toLowerCase())) {
        toast.error("A function with this name already exists");
        return;
      }
    }

    setLoading(true);
    try {
      if (editingFunction) {
        // Update existing function
        await axios.put(`${API}/custom-functions/${editingFunction}`, newFunction);
        toast.success("Function updated successfully!");
      } else {
        // Create new function
        await axios.post(`${API}/custom-functions`, newFunction);
        toast.success("Custom function saved successfully!");
      }
      
      // Reset form
      resetForm();
      setShowBuilder(false);
      
      // Reload functions
      loadCustomFunctions();
      
      // Notify parent
      if (onFunctionSaved) {
        onFunctionSaved();
      }
    } catch (error) {
      toast.error(error.response?.data?.detail || "Failed to save function");
    } finally {
      setLoading(false);
    }
  };

  const { confirmProps, openConfirm } = useAppDialog();

  const handleDeleteFunction = async (functionId, functionName) => {
    openConfirm({
      title: "Delete Function",
      message: `Are you sure you want to delete "${functionName}"?`,
      confirmLabel: "Delete",
      confirmColor: "error",
      onConfirm: async () => {
        try {
          await axios.delete(`${API}/custom-functions/${functionId}`);
          toast.success("Function deleted successfully!");
          loadCustomFunctions();
          if (onFunctionSaved) {
            onFunctionSaved();
          }
        } catch (error) {
          toast.error("Failed to delete function");
        }
      }
    });
  };

  const formatParams = (params) => {
    return params.map(p => `${p.name}: ${p.type}`).join(", ");
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50" data-testid="custom-function-builder-modal">
      <div className="bg-white rounded-xl shadow-2xl w-[900px] max-h-[90vh] flex flex-col">
        {/* Header */}
          <div className="px-6 py-4 border-b border-slate-200 flex items-center justify-between bg-gradient-to-r from-purple-50 to-indigo-50">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-purple-500 to-indigo-600 flex items-center justify-center">
                <Brackets className="w-5 h-5 text-white" />
              </div>
              <div>
                <h2 className="text-xl font-bold text-slate-900" style={{ fontFamily: 'Manrope' }}>
                  Custom Function Builder
                </h2>
                <p className="text-sm text-slate-600">Create and manage your own DSL functions</p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button 
                    variant="outline" 
                    size="sm" 
                    onClick={() => setShowDocumentation(!showDocumentation)}
                    className="text-blue-600 border-blue-200 hover:bg-blue-50"
                  >
                    <HelpCircle className="w-4 h-4 mr-1" />
                    Help
                  </Button>
                </TooltipTrigger>
                <TooltipContent>
                  <p>View documentation on how to create functions</p>
                </TooltipContent>
              </Tooltip>
              <Button variant="ghost" size="icon" onClick={onClose} data-testid="close-function-builder">
                <X className="w-5 h-5" />
              </Button>
            </div>
          </div>

          <div className="flex-1 overflow-hidden flex relative">
            {/* Documentation Overlay */}
            {showDocumentation && (
              <DocumentationGuide onClose={() => setShowDocumentation(false)} />
            )}
            
            {/* Left Panel - Existing Custom Functions */}
            <div className="w-1/2 border-r border-slate-200 flex flex-col">
              <div className="p-4 border-b border-slate-100 flex items-center justify-between">
                <h3 className="font-semibold text-slate-800">Your Custom Functions</h3>
                <Badge variant="secondary">{customFunctions.length}</Badge>
              </div>
              
              <ScrollArea className="flex-1 p-4">
                {customFunctions.length === 0 ? (
                  <div className="text-center py-8 text-slate-500">
                    <FileCode className="w-12 h-12 mx-auto mb-3 text-slate-300" />
                    <p className="text-sm">No custom functions yet</p>
                    <p className="text-xs mt-1">Click "New Function" to create one</p>
                    <Button 
                      variant="link" 
                      size="sm" 
                      onClick={() => setShowDocumentation(true)}
                      className="mt-2 text-blue-600"
                    >
                      <BookOpen className="w-4 h-4 mr-1" />
                      Read the guide
                    </Button>
                  </div>
                ) : (
                  <div className="space-y-3">
                    {customFunctions.map((func) => (
                      <Card 
                        key={func.id} 
                        className="p-3 hover:shadow-md transition-shadow cursor-pointer"
                        onClick={() => setExpandedFunction(expandedFunction === func.id ? null : func.id)}
                      >
                        <div className="flex items-start justify-between">
                          <div className="flex-1">
                            <div className="flex items-center gap-2">
                              <span className="font-mono text-sm font-semibold text-purple-700">{func.name}</span>
                              <Badge variant="outline" className="text-xs">{func.category}</Badge>
                            </div>
                            <p className="text-xs text-slate-600 mt-1 line-clamp-1">{func.description}</p>
                          </div>
                          <div className="flex items-center gap-1">
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Button 
                                  variant="ghost" 
                                  size="icon" 
                                  className="h-7 w-7 text-blue-500 hover:text-blue-700 hover:bg-blue-50"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    handleEditFunction(func);
                                  }}
                                  data-testid={`edit-function-${func.name}`}
                                >
                                  <Edit2 className="w-4 h-4" />
                                </Button>
                              </TooltipTrigger>
                              <TooltipContent>
                                <p>Edit function</p>
                              </TooltipContent>
                            </Tooltip>
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Button 
                                  variant="ghost" 
                                  size="icon" 
                                  className="h-7 w-7 text-red-500 hover:text-red-700 hover:bg-red-50"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    handleDeleteFunction(func.id, func.name);
                                  }}
                                  data-testid={`delete-function-${func.name}`}
                                >
                                  <Trash2 className="w-4 h-4" />
                                </Button>
                              </TooltipTrigger>
                              <TooltipContent>
                                <p>Delete function</p>
                              </TooltipContent>
                            </Tooltip>
                            {expandedFunction === func.id ? (
                              <ChevronUp className="w-4 h-4 text-slate-400" />
                            ) : (
                              <ChevronDown className="w-4 h-4 text-slate-400" />
                            )}
                          </div>
                        </div>
                        
                        {expandedFunction === func.id && (
                          <div className="mt-3 pt-3 border-t border-slate-100">
                            <div className="space-y-2">
                              <div>
                                <span className="text-xs font-medium text-slate-500">Signature:</span>
                                <code className="block text-xs bg-slate-50 p-2 rounded mt-1 font-mono">
                                  {func.name}({formatParams(func.parameters)}) → {func.return_type}
                                </code>
                              </div>
                              <div>
                                <span className="text-xs font-medium text-slate-500">Formula:</span>
                                <code className="block text-xs bg-slate-900 text-green-400 p-2 rounded mt-1 font-mono whitespace-pre-wrap">
                                  {func.formula}
                                </code>
                              </div>
                              {func.example && (
                                <div>
                                  <span className="text-xs font-medium text-slate-500">Example:</span>
                                  <code className="block text-xs bg-blue-50 text-blue-700 p-2 rounded mt-1 font-mono">
                                    {func.example}
                                  </code>
                                </div>
                              )}
                            </div>
                          </div>
                        )}
                      </Card>
                    ))}
                  </div>
                )}
              </ScrollArea>
              
              <div className="p-4 border-t border-slate-200">
                <Button 
                  onClick={() => { resetForm(); setShowBuilder(true); }} 
                  className="w-full bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-700 hover:to-indigo-700"
                  data-testid="new-function-button"
                >
                  <Plus className="w-4 h-4 mr-2" />
                  New Function
                </Button>
              </div>
            </div>

            {/* Right Panel - Function Builder Form */}
            <div className="w-1/2 flex flex-col">
              {!showBuilder ? (
                <div className="flex-1 flex items-center justify-center text-slate-500 p-8">
                  <div className="text-center">
                    <Code className="w-16 h-16 mx-auto mb-4 text-slate-300" />
                    <p className="text-lg font-medium">Build Your Function</p>
                    <p className="text-sm mt-2">Click "New Function" to start creating a custom DSL function</p>
                    <Button 
                      variant="link" 
                      size="sm" 
                      onClick={() => setShowDocumentation(true)}
                      className="mt-4 text-blue-600"
                    >
                      <BookOpen className="w-4 h-4 mr-1" />
                      Read the documentation
                    </Button>
                  </div>
                </div>
              ) : (
                <>
                  <div className="p-4 border-b border-slate-100 flex items-center justify-between bg-purple-50">
                    <h3 className="font-semibold text-purple-800">
                      {editingFunction ? "Edit Function" : "New Custom Function"}
                    </h3>
                    <Button 
                      variant="ghost" 
                      size="sm" 
                      onClick={() => { setShowBuilder(false); resetForm(); }}
                      className="text-purple-600"
                    >
                      Cancel
                    </Button>
                  </div>
                  
                  <ScrollArea className="flex-1 p-4">
                    <div className="space-y-4">
                      {/* Function Name */}
                      <div>
                        <label className="text-sm font-medium text-slate-700 mb-1 flex items-center gap-1">
                          Function Name <span className="text-red-500">*</span>
                          <Tooltip>
                          <Tooltip title="Use snake_case: lowercase letters with underscores. Example: calculate_interest">
                            <HelpCircle className="w-3 h-3 text-slate-400" />
                          </Tooltip>
                        </label>
                        <TextField
                          value={newFunction.name}
                          onChange={(e) => setNewFunction(prev => ({ ...prev, name: e.target.value.replace(/\s/g, '_') }))}
                          placeholder="my_custom_function"
                          size="small"
                          fullWidth
                          disabled={!!editingFunction}
                          data-testid="function-name-input"
                          InputProps={{ sx: { fontFamily: 'monospace' } }}
                        />
                        <p className="text-xs text-slate-500 mt-1">Use lowercase with underscores (snake_case)</p>
                      </div>

                      {/* Category */}
                      <div>
                        <label className="text-sm font-medium text-slate-700 mb-1 flex items-center gap-1">
                          Category
                          <Tooltip>
                            <TooltipTrigger>
                              <HelpCircle className="w-3 h-3 text-slate-400" />
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>Group your function for easier discovery in the function browser</p>
                            </TooltipContent>
                          </Tooltip>
                        </label>
                        <select
                          value={newFunction.category}
                          onChange={(e) => setNewFunction(prev => ({ ...prev, category: e.target.value }))}
                          className="w-full border border-slate-200 rounded-md p-2 text-sm"
                          data-testid="function-category-select"
                        >
                          {CATEGORIES.map(cat => (
                            <option key={cat} value={cat}>{cat}</option>
                          ))}
                        </select>
                      </div>

                      {/* Description */}
                      <div>
                        <label className="text-sm font-medium text-slate-700 mb-1 flex items-center gap-1">
                          Description <span className="text-red-500">*</span>
                          <Tooltip>
                            <TooltipTrigger>
                              <HelpCircle className="w-3 h-3 text-slate-400" />
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>Briefly explain what your function does</p>
                            </TooltipContent>
                          </Tooltip>
                        </label>
                        <TextField
                          value={newFunction.description}
                          onChange={(e) => setNewFunction(prev => ({ ...prev, description: e.target.value }))}
                          placeholder="What does this function do?"
                          multiline
                          rows={2}
                          fullWidth
                          size="small"
                          data-testid="function-description-input"
                        />
                      </div>

                      {/* Parameters */}
                      <div>
                        <div className="flex items-center justify-between mb-2">
                          <label className="text-sm font-medium text-slate-700 flex items-center gap-1">
                            Parameters
                            <Tooltip>
                              <TooltipTrigger>
                                <HelpCircle className="w-3 h-3 text-slate-400" />
                              </TooltipTrigger>
                              <TooltipContent className="max-w-xs">
                                <p>Input values your function needs. Use descriptive names like 'principal', 'rate', 'periods'</p>
                              </TooltipContent>
                            </Tooltip>
                          </label>
                          <Button 
                            variant="outline" 
                            size="sm" 
                            onClick={handleAddParameter}
                            className="h-7 text-xs"
                            data-testid="add-parameter-button"
                          >
                            <Plus className="w-3 h-3 mr-1" />
                            Add Parameter
                          </Button>
                        </div>
                        
                        <div className="space-y-2">
                          {newFunction.parameters.map((param, index) => (
                            <div key={index} className="flex gap-2 items-start bg-slate-50 p-2 rounded">
                              <TextField
                                value={param.name}
                                onChange={(e) => handleParameterChange(index, 'name', e.target.value)}
                                placeholder="param_name"
                                size="small"
                                sx={{ flex: 1 }}
                                data-testid={`param-name-${index}`}
                                InputProps={{ sx: { fontFamily: 'monospace', fontSize: '0.875rem' } }}
                              />
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <select
                                    value={param.type}
                                    onChange={(e) => handleParameterChange(index, 'type', e.target.value)}
                                    className="w-24 border border-slate-200 rounded-md p-2 text-sm"
                                    data-testid={`param-type-${index}`}
                                  >
                                    {RETURN_TYPES.map(type => (
                                      <option key={type} value={type}>{type}</option>
                                    ))}
                                  </select>
                                </TooltipTrigger>
                                <TooltipContent>
                                  <p>decimal: numbers, string: text, date: dates, boolean: true/false</p>
                                </TooltipContent>
                              </Tooltip>
                              {newFunction.parameters.length > 1 && (
                                <Button 
                                  variant="ghost" 
                                  size="icon"
                                  className="h-9 w-9 text-red-500 hover:text-red-700"
                                  onClick={() => handleRemoveParameter(index)}
                                >
                                  <Trash2 className="w-4 h-4" />
                                </Button>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>

                      {/* Return Type */}
                      <div>
                        <label className="text-sm font-medium text-slate-700 mb-1 flex items-center gap-1">
                          Return Type
                          <Tooltip>
                            <TooltipTrigger>
                              <HelpCircle className="w-3 h-3 text-slate-400" />
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>What type of value your function returns</p>
                            </TooltipContent>
                          </Tooltip>
                        </label>
                        <select
                          value={newFunction.returnType}
                          onChange={(e) => setNewFunction(prev => ({ ...prev, returnType: e.target.value }))}
                          className="w-full border border-slate-200 rounded-md p-2 text-sm"
                          data-testid="function-return-type-select"
                        >
                          {RETURN_TYPES.map(type => (
                            <option key={type} value={type}>{type}</option>
                          ))}
                        </select>
                      </div>

                      {/* Formula/Logic */}
                      <div>
                        <label className="text-sm font-medium text-slate-700 mb-1 flex items-center gap-1">
                          Formula / Logic <span className="text-red-500">*</span>
                          <Tooltip>
                            <TooltipTrigger>
                              <HelpCircle className="w-3 h-3 text-slate-400" />
                            </TooltipTrigger>
                            <TooltipContent className="max-w-xs">
                              <p>Write your calculation using parameter names. Start with 'return' for the result.</p>
                            </TooltipContent>
                          </Tooltip>
                        </label>
                        <TextField
                          value={newFunction.formula}
                          onChange={(e) => setNewFunction(prev => ({ ...prev, formula: e.target.value }))}
                          placeholder="return principal * rate * time"
                          multiline
                          rows={4}
                          fullWidth
                          size="small"
                          data-testid="function-formula-input"
                          InputProps={{ sx: { fontFamily: 'monospace', fontSize: '0.875rem' } }}
                        />
                        <div className="flex items-start gap-2 mt-2 p-2 bg-amber-50 rounded text-xs text-amber-700">
                          <Info className="w-4 h-4 flex-shrink-0 mt-0.5" />
                          <span>
                            Use parameter names directly. The formula will be executed as Python code.
                            Example: <code className="bg-amber-100 px-1 rounded">return principal * (1 + rate) ** years</code>
                          </span>
                        </div>
                      </div>

                      {/* Example Usage */}
                      <div>
                        <label className="text-sm font-medium text-slate-700 mb-1 flex items-center gap-1">
                          Example Usage (optional)
                          <Tooltip>
                            <TooltipTrigger>
                              <HelpCircle className="w-3 h-3 text-slate-400" />
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>Show how to call your function with sample values</p>
                            </TooltipContent>
                          </Tooltip>
                        </label>
                        <TextField
                          value={newFunction.example}
                          onChange={(e) => setNewFunction(prev => ({ ...prev, example: e.target.value }))}
                          placeholder="result = my_custom_function(1000, 0.05, 12)"
                          size="small"
                          fullWidth
                          data-testid="function-example-input"
                          InputProps={{ sx: { fontFamily: 'monospace', fontSize: '0.875rem' } }}
                        />
                      </div>
                    </div>
                  </ScrollArea>

                  <div className="p-4 border-t border-slate-200 bg-slate-50">
                    <Button 
                      onClick={handleSaveFunction}
                      disabled={loading}
                      className="w-full bg-gradient-to-r from-purple-600 to-indigo-600 hover:from-purple-700 hover:to-indigo-700"
                      data-testid="save-function-button"
                    >
                      <Save className="w-4 h-4 mr-2" />
                      {loading ? "Saving..." : (editingFunction ? "Update Function" : "Save Function")}
                    </Button>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      </div>
      <AppDialog {...confirmProps} />
    </div>
  );
};

export default CustomFunctionBuilder;
