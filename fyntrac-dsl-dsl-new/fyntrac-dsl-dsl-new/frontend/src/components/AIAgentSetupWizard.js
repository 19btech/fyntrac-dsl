import React, { useState, useEffect } from "react";
import axios from "axios";
import {
  Dialog, DialogTitle, DialogContent, DialogActions,
  Button, TextField, Box, Typography, IconButton,
  Stepper, Step, StepLabel, CircularProgress, InputAdornment,
  Card, CardContent, CardActionArea, Alert, Chip,
} from "@mui/material";
import { X, Eye, EyeOff, Check, AlertTriangle, ExternalLink, Unplug, RefreshCw } from "lucide-react";
import { useToast } from "./ToastProvider";
import { API } from "../config";

// Inline SVG logos
const GeminiLogo = () => (
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
    <path d="M12 2C12 2 14.5 6.5 18 8.5C14.5 10.5 12 15 12 15C12 15 9.5 10.5 6 8.5C9.5 6.5 12 2 12 2Z" fill="#4285F4"/>
    <path d="M12 9C12 9 13.5 12 16 13.5C13.5 15 12 18 12 18C12 18 10.5 15 8 13.5C10.5 12 12 9 12 9Z" fill="#34A853"/>
    <path d="M12 15C12 15 12.8 17 14.5 18C12.8 19 12 21 12 21C12 21 11.2 19 9.5 18C11.2 17 12 15 12 15Z" fill="#FBBC05"/>
  </svg>
);

const OpenAILogo = () => (
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
    <path d="M22.282 9.821a5.985 5.985 0 0 0-.516-4.91 6.046 6.046 0 0 0-6.51-2.9A6.065 6.065 0 0 0 4.981 4.18a5.998 5.998 0 0 0-3.998 2.9 6.046 6.046 0 0 0 .743 7.097 5.98 5.98 0 0 0 .51 4.911 6.051 6.051 0 0 0 6.515 2.9A5.985 5.985 0 0 0 13.26 24a6.056 6.056 0 0 0 5.772-4.206 5.99 5.99 0 0 0 3.997-2.9 6.056 6.056 0 0 0-.747-7.073zM13.26 22.43a4.476 4.476 0 0 1-2.876-1.04l.141-.081 4.779-2.758a.795.795 0 0 0 .392-.681v-6.737l2.02 1.168a.071.071 0 0 1 .038.052v5.583a4.504 4.504 0 0 1-4.494 4.494zM3.6 18.304a4.47 4.47 0 0 1-.535-3.014l.142.085 4.783 2.759a.771.771 0 0 0 .78 0l5.843-3.369v2.332a.08.08 0 0 1-.033.062L9.74 19.95a4.5 4.5 0 0 1-6.14-1.646zM2.34 7.896a4.485 4.485 0 0 1 2.366-1.973V11.6a.766.766 0 0 0 .388.676l5.815 3.355-2.02 1.168a.076.076 0 0 1-.071 0l-4.83-2.786A4.504 4.504 0 0 1 2.34 7.872zm16.597 3.855l-5.833-3.387L15.119 7.2a.076.076 0 0 1 .071 0l4.83 2.791a4.494 4.494 0 0 1-.676 8.105v-5.678a.79.79 0 0 0-.407-.667zm2.01-3.023l-.141-.085-4.774-2.782a.776.776 0 0 0-.785 0L9.409 9.23V6.897a.066.066 0 0 1 .028-.061l4.83-2.787a4.5 4.5 0 0 1 6.68 4.66zm-12.64 4.135l-2.02-1.164a.08.08 0 0 1-.038-.057V6.075a4.5 4.5 0 0 1 7.375-3.453l-.142.08L8.704 5.46a.795.795 0 0 0-.393.681zm1.097-2.365l2.602-1.5 2.607 1.5v2.999l-2.597 1.5-2.607-1.5z" fill="#10A37F"/>
  </svg>
);

const AnthropicLogo = () => (
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
    <path d="M13.827 3.52h3.603L24 20.48h-3.603l-6.57-16.96zm-7.258 0h3.767L16.906 20.48h-3.674l-1.587-4.227H5.246l-1.579 4.227H0L6.569 3.52zm1.04 3.845L5.2 13.298h4.818L7.609 7.365z" fill="#D97757"/>
  </svg>
);

const DeepSeekLogo = () => (
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
    <circle cx="12" cy="12" r="10" fill="#4D6BFE"/>
    <path d="M8 12.5c0-2.5 1.5-4 4-4s4 1.5 4 4-1.5 4-4 4" stroke="#fff" strokeWidth="2" strokeLinecap="round" fill="none"/>
    <circle cx="12" cy="8.5" r="1.2" fill="#fff"/>
  </svg>
);

const PROVIDER_LOGOS = { gemini: GeminiLogo, openai: OpenAILogo, anthropic: AnthropicLogo, deepseek: DeepSeekLogo };

const PROVIDERS = {
  gemini: {
    name: "Google Gemini",
    description: "Google's multimodal AI models",
    keyUrl: "https://aistudio.google.com/apikey",
    color: "#4285F4",
  },
  openai: {
    name: "OpenAI (ChatGPT)",
    description: "GPT-4o, GPT-4, and more",
    keyUrl: "https://platform.openai.com/api-keys",
    color: "#10A37F",
  },
  anthropic: {
    name: "Anthropic (Claude)",
    description: "Claude 3.5 Sonnet, Opus, and Haiku",
    keyUrl: "https://console.anthropic.com/settings/keys",
    color: "#D97757",
  },
  deepseek: {
    name: "DeepSeek",
    description: "DeepSeek-V3, DeepSeek-R1, and more",
    keyUrl: "https://platform.deepseek.com/api_keys",
    color: "#4D6BFE",
  },
};

const STEPS = ["Select Platform", "Enter API Key", "Test Connection", "Save"];

const AIAgentSetupWizard = ({ open, onClose, onSaved }) => {
  const toast = useToast();
  const [mode, setMode] = useState("loading"); // "loading" | "status" | "setup"
  const [currentConfig, setCurrentConfig] = useState(null);
  const [step, setStep] = useState(0);
  const [selectedProvider, setSelectedProvider] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [showKey, setShowKey] = useState(false);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [saving, setSaving] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const [selectedModel, setSelectedModel] = useState("");

  // Load current status when dialog opens
  useEffect(() => {
    if (open) {
      setStep(0);
      setSelectedProvider("");
      setApiKey("");
      setShowKey(false);
      setTesting(false);
      setTestResult(null);
      setSaving(false);
      setSelectedModel("");
      setDisconnecting(false);
      // Check if already configured
      axios.get(`${API}/ai/provider/status`)
        .then(res => {
          if (res.data.configured) {
            setCurrentConfig(res.data);
            setMode("status");
          } else {
            setCurrentConfig(null);
            setMode("setup");
          }
        })
        .catch(() => {
          setCurrentConfig(null);
          setMode("setup");
        });
    }
  }, [open]);

  const handleDisconnect = async () => {
    setDisconnecting(true);
    try {
      await axios.delete(`${API}/ai/provider`);
      toast.success("AI provider disconnected");
      setCurrentConfig(null);
      setMode("setup");
      if (onSaved) onSaved(); // Trigger refresh so ModelSelector updates
    } catch {
      toast.error("Failed to disconnect provider");
    } finally {
      setDisconnecting(false);
    }
  };

  const handleReconfigure = () => {
    setMode("setup");
  };

  const handleTestConnection = async () => {
    setTesting(true);
    setTestResult(null);
    try {
      const response = await axios.post(`${API}/ai/provider/test`, {
        provider: selectedProvider,
        api_key: apiKey,
      });
      setTestResult(response.data);
      if (response.data.valid && response.data.models?.length > 0) {
        setSelectedModel(response.data.models[0].id);
      }
    } catch (error) {
      setTestResult({
        valid: false,
        error_type: "network",
        error_message: "Unable to reach the server. Check your connection.",
        models: [],
      });
    } finally {
      setTesting(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      await axios.post(`${API}/ai/provider/save`, {
        provider: selectedProvider,
        api_key: apiKey,
        selected_model: selectedModel,
        available_models: testResult?.models || [],
      });
      toast.success("AI provider configured successfully!");
      if (onSaved) onSaved();
      onClose();
    } catch (error) {
      toast.error("Failed to save configuration");
    } finally {
      setSaving(false);
    }
  };

  const canProceed = () => {
    switch (step) {
      case 0: return !!selectedProvider;
      case 1: return apiKey.trim().length > 0;
      case 2: return testResult?.valid && selectedModel;
      case 3: return true;
      default: return false;
    }
  };

  const ProviderLogo = ({ provider, size = 24 }) => {
    const Logo = PROVIDER_LOGOS[provider];
    return Logo ? <Logo /> : null;
  };

  // --- Connected status view ---
  const renderStatus = () => {
    if (!currentConfig) return null;
    const prov = PROVIDERS[currentConfig.provider] || {};
    const modelName = currentConfig.available_models?.find(m => m.id === currentConfig.selected_model)?.name || currentConfig.selected_model;
    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 3, mt: 1 }}>
        <Alert severity="success" icon={<Check size={18} />}>
          AI provider is connected and ready to use.
        </Alert>

        <Card variant="outlined" sx={{ borderColor: prov.color, borderWidth: 2 }}>
          <CardContent sx={{ display: "flex", alignItems: "center", gap: 2 }}>
            <Box sx={{ width: 48, height: 48, borderRadius: 2, bgcolor: "#F8F9FA", display: "flex", alignItems: "center", justifyContent: "center" }}>
              <ProviderLogo provider={currentConfig.provider} />
            </Box>
            <Box sx={{ flex: 1 }}>
              <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                {prov.name || currentConfig.provider}
              </Typography>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1, mt: 0.5 }}>
                <Chip label={modelName} size="small" sx={{ bgcolor: "#EEF0FE", color: "#5B5FED", fontWeight: 500 }} />
                <Chip label="Connected" size="small" color="success" variant="outlined" />
              </Box>
            </Box>
          </CardContent>
        </Card>

        <Box sx={{ display: "flex", gap: 2 }}>
          <Button
            variant="outlined"
            startIcon={<RefreshCw size={16} />}
            onClick={handleReconfigure}
            sx={{ flex: 1 }}
          >
            Change Provider
          </Button>
          <Button
            variant="outlined"
            color="error"
            startIcon={disconnecting ? <CircularProgress size={16} color="inherit" /> : <Unplug size={16} />}
            onClick={handleDisconnect}
            disabled={disconnecting}
            sx={{ flex: 1 }}
          >
            {disconnecting ? "Disconnecting..." : "Disconnect"}
          </Button>
        </Box>
      </Box>
    );
  };

  // --- Setup wizard steps ---
  const renderStep = () => {
    switch (step) {
      case 0:
        return (
          <Box sx={{ display: "flex", flexDirection: "column", gap: 2, mt: 2 }}>
            <Typography variant="body2" color="text.secondary">
              Choose your AI provider. You can change this later.
            </Typography>
            {Object.entries(PROVIDERS).map(([key, p]) => (
              <Card
                key={key}
                variant={selectedProvider === key ? "outlined" : "elevation"}
                sx={{
                  borderColor: selectedProvider === key ? p.color : "transparent",
                  borderWidth: 2,
                  borderStyle: "solid",
                  boxShadow: selectedProvider === key ? `0 0 0 1px ${p.color}` : 1,
                }}
              >
                <CardActionArea onClick={() => setSelectedProvider(key)} sx={{ p: 2 }}>
                  <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
                    <Box
                      sx={{
                        width: 40, height: 40, borderRadius: 2,
                        bgcolor: "#F8F9FA", display: "flex", alignItems: "center",
                        justifyContent: "center",
                      }}
                    >
                      <ProviderLogo provider={key} />
                    </Box>
                    <Box sx={{ flex: 1 }}>
                      <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                        {p.name}
                      </Typography>
                      <Typography variant="body2" color="text.secondary">
                        {p.description}
                      </Typography>
                    </Box>
                    {selectedProvider === key && (
                      <Check size={20} color={p.color} />
                    )}
                  </Box>
                </CardActionArea>
              </Card>
            ))}
          </Box>
        );

      case 1: {
        const provider = PROVIDERS[selectedProvider];
        return (
          <Box sx={{ display: "flex", flexDirection: "column", gap: 3, mt: 2 }}>
            <Typography variant="body2" color="text.secondary">
              Enter your personal API key for {provider?.name}.
            </Typography>
            <TextField
              fullWidth
              label="API Key"
              type={showKey ? "text" : "password"}
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              autoFocus
              InputProps={{
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton size="small" onClick={() => setShowKey(!showKey)}>
                      {showKey ? <EyeOff size={18} /> : <Eye size={18} />}
                    </IconButton>
                  </InputAdornment>
                ),
              }}
            />
            <Button
              size="small"
              variant="text"
              href={provider?.keyUrl}
              target="_blank"
              rel="noopener noreferrer"
              startIcon={<ExternalLink size={14} />}
              sx={{ alignSelf: "flex-start", textTransform: "none" }}
            >
              Where do I get my API key?
            </Button>
          </Box>
        );
      }

      case 2:
        return (
          <Box sx={{ display: "flex", flexDirection: "column", gap: 3, mt: 2 }}>
            <Typography variant="body2" color="text.secondary">
              Let's verify your API key works and discover available models.
            </Typography>

            <Button
              variant="contained"
              onClick={handleTestConnection}
              disabled={testing}
              startIcon={testing ? <CircularProgress size={16} color="inherit" /> : null}
            >
              {testing ? "Testing..." : "Test Connection"}
            </Button>

            {testResult && (
              <>
                {testResult.valid ? (
                  <Alert severity="success" icon={<Check size={18} />}>
                    Connected! {testResult.models?.length || 0} models available.
                  </Alert>
                ) : (
                  <Alert severity="error" icon={<AlertTriangle size={18} />}>
                    {testResult.error_message || "Connection failed."}
                  </Alert>
                )}

                {testResult.valid && testResult.models?.length > 0 && (
                  <Box>
                    <Typography variant="body2" sx={{ mb: 1, fontWeight: 600 }}>
                      Select a default model:
                    </Typography>
                    <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
                      {testResult.models.map((m) => (
                        <Card
                          key={m.id}
                          variant={selectedModel === m.id ? "outlined" : "elevation"}
                          sx={{
                            borderColor:
                              selectedModel === m.id
                                ? PROVIDERS[selectedProvider]?.color
                                : "transparent",
                            borderWidth: 2,
                            borderStyle: "solid",
                            cursor: "pointer",
                          }}
                          onClick={() => setSelectedModel(m.id)}
                        >
                          <CardContent sx={{ py: 1.5, px: 2, "&:last-child": { pb: 1.5 } }}>
                            <Box sx={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                              <Typography variant="body2" sx={{ fontWeight: 500 }}>
                                {m.name}
                              </Typography>
                              {selectedModel === m.id && (
                                <Check size={16} color={PROVIDERS[selectedProvider]?.color} />
                              )}
                            </Box>
                          </CardContent>
                        </Card>
                      ))}
                    </Box>
                  </Box>
                )}
              </>
            )}
          </Box>
        );

      case 3:
        return (
          <Box sx={{ display: "flex", flexDirection: "column", gap: 2, mt: 2 }}>
            <Alert severity="info">
              Ready to save your configuration.
            </Alert>
            <Box sx={{ bgcolor: "#F8F9FA", borderRadius: 2, p: 2 }}>
              <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>Summary</Typography>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 0.5 }}>
                <ProviderLogo provider={selectedProvider} />
                <Typography variant="body2">
                  {PROVIDERS[selectedProvider]?.name}
                </Typography>
              </Box>
              <Typography variant="body2">
                Model: {testResult?.models?.find(m => m.id === selectedModel)?.name || selectedModel}
              </Typography>
              <Typography variant="body2">
                API Key: ••••{apiKey.slice(-4)}
              </Typography>
            </Box>
          </Box>
        );

      default:
        return null;
    }
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="sm"
      fullWidth
      PaperProps={{ sx: { borderRadius: 3 } }}
    >
      <DialogTitle sx={{ display: "flex", alignItems: "center", justifyContent: "space-between", pb: 1 }}>
        <Typography variant="h6" sx={{ fontWeight: 600 }}>AI Agent Setup</Typography>
        <IconButton size="small" onClick={onClose}>
          <X size={18} />
        </IconButton>
      </DialogTitle>

      <DialogContent>
        {mode === "loading" && (
          <Box sx={{ display: "flex", justifyContent: "center", py: 4 }}>
            <CircularProgress />
          </Box>
        )}

        {mode === "status" && renderStatus()}

        {mode === "setup" && (
          <>
            <Stepper activeStep={step} alternativeLabel sx={{ mb: 2 }}>
              {STEPS.map((label) => (
                <Step key={label}>
                  <StepLabel>{label}</StepLabel>
                </Step>
              ))}
            </Stepper>
            {renderStep()}
          </>
        )}
      </DialogContent>

      {mode === "setup" && (
        <DialogActions sx={{ px: 3, pb: 2 }}>
          {step > 0 && (
            <Button onClick={() => setStep(step - 1)} disabled={saving}>
              Back
            </Button>
          )}
          <Box sx={{ flex: 1 }} />
          {step < 3 ? (
            <Button
              variant="contained"
              onClick={() => setStep(step + 1)}
              disabled={!canProceed()}
            >
              Next
            </Button>
          ) : (
            <Button
              variant="contained"
              onClick={handleSave}
              disabled={saving}
              startIcon={saving ? <CircularProgress size={16} color="inherit" /> : null}
            >
              {saving ? "Saving..." : "Save Configuration"}
            </Button>
          )}
        </DialogActions>
      )}

      {mode === "status" && (
        <DialogActions sx={{ px: 3, pb: 2 }}>
          <Button onClick={onClose}>Close</Button>
        </DialogActions>
      )}
    </Dialog>
  );
};

export default AIAgentSetupWizard;
