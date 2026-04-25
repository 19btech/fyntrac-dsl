import React, { useState, useMemo } from "react";
import { Dialog, DialogContent, DialogTitle, Card, CardContent, Button, TextField, IconButton, InputAdornment, Chip, Box, Typography, Tooltip } from '@mui/material';
import { Search, BookOpen, Copy, X, Sparkles } from "lucide-react";
import { useToast } from "./ToastProvider";
import { getExplanation } from "../agent/testing/explanationStore";

const FunctionBrowser = ({ dslFunctions, onClose, onAskAI }) => {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedCategory, setSelectedCategory] = useState("All");
  const toast = useToast();

  const categories = useMemo(() => {
    const cats = ["All", ...new Set(dslFunctions.map(f => f.category))];
    return cats.filter(Boolean);
  }, [dslFunctions]);

  const customCount = useMemo(() => {
    return dslFunctions.filter(f => f.is_custom).length;
  }, [dslFunctions]);

  const filteredFunctions = useMemo(() => {
    return dslFunctions.filter(func => {
      const matchesSearch = !searchQuery || 
        func.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        func.description.toLowerCase().includes(searchQuery.toLowerCase());
      
      const matchesCategory = selectedCategory === "All" || func.category === selectedCategory;
      
      return matchesSearch && matchesCategory;
    });
  }, [dslFunctions, searchQuery, selectedCategory]);

  const handleCopyFunction = (func) => {
    const functionCall = `${func.name}(${func.params})`;
    navigator.clipboard.writeText(functionCall);
    toast.success(`Copied: ${functionCall}`);
  };

  return (
    <Dialog 
      open={true} 
      onClose={onClose} 
      maxWidth="lg" 
      fullWidth
      PaperProps={{ sx: { height: '85vh' } }}
      data-testid="function-browser"
    >
      <DialogTitle>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
            <BookOpen size={24} color="#5B5FED" />
            <Box>
              <Typography variant="h4">Formula Library</Typography>
              <Typography variant="body2" color="text.secondary">
                {dslFunctions.length} formulas available
                {customCount > 0 && (
                  <Box component="span" sx={{ ml: 1, color: '#7C3AED' }}>
                    ({customCount} user-created)
                  </Box>
                )}
              </Typography>
            </Box>
          </Box>
          <IconButton onClick={onClose} data-testid="close-browser">
            <X size={20} />
          </IconButton>
        </Box>
      </DialogTitle>

      <DialogContent sx={{ display: 'flex', flexDirection: 'column', p: 3 }}>
        <Box sx={{ mb: 3 }}>
          <TextField
            placeholder="Search formulas by name or description..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            fullWidth
            size="small"
            data-testid="function-search-input"
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Search size={16} color="#6C757D" />
                </InputAdornment>
              ),
            }}
            sx={{ mb: 2 }}
          />

          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
            {categories.map(category => (
              <Chip
                key={category}
                label={category}
                onClick={() => setSelectedCategory(category)}
                sx={{ 
                  cursor: 'pointer',
                  bgcolor: selectedCategory === category ? '#EEF0FE' : '#FFFFFF',
                  color: selectedCategory === category ? '#14213d' : '#495057',
                  border: selectedCategory === category ? '1px solid #E9E6FB' : '1px solid #ECECEC',
                  boxShadow: 'none',
                  '&:hover': {
                    bgcolor: selectedCategory === category ? '#E9ECFD' : '#F8F9FA',
                  },
                  '&:focus, &:focus-visible': {
                    outline: 'none',
                    boxShadow: 'none',
                  }
                }}
                data-testid={`category-${category}`}
              />
            ))}
          </Box>

          <Typography variant="body2" color="text.secondary">
            Showing {filteredFunctions.length} of {dslFunctions.length} formulas
          </Typography>
        </Box>

        <Box sx={{ flex: 1, overflowY: 'auto' }}>
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(2, 1fr)' }, gap: 2 }}>
            {filteredFunctions.map((func, idx) => (
              <Card 
                key={idx}
                sx={{ 
                  borderLeft: func.is_custom ? '4px solid #A855F7' : '1px solid #E9ECEF',
                }} 
                data-testid={`function-card-${func.name}`}
              >
                <CardContent sx={{ p: 2 }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1.5 }}>
                    <Box sx={{ flex: 1 }}>
                      <Typography variant="h6" sx={{ fontFamily: 'monospace', fontSize: '0.9375rem', mb: 0.5 }}>
                        {func.name}({func.params})
                      </Typography>
                      <Chip 
                        label={func.category} 
                        size="small"
                        sx={{ 
                          bgcolor: '#EEF0FE', 
                          color: '#5B5FED',
                          fontSize: '0.6875rem',
                          height: 18
                        }}
                      />
                      {func.is_custom && (
                        <Chip
                          icon={<Sparkles size={10} />}
                          label="User-Created"
                          size="small"
                          sx={{ 
                            ml: 0.5,
                            bgcolor: '#F3E8FF', 
                            color: '#7C3AED',
                            fontSize: '0.6875rem',
                            height: 18
                          }}
                        />
                      )}
                    </Box>
                    <Tooltip title="Copy">
                      <span>
                        <IconButton
                          size="small"
                          onClick={() => handleCopyFunction(func)}
                          data-testid={`copy-${func.name}`}
                        >
                          <Copy size={14} />
                        </IconButton>
                      </span>
                    </Tooltip>
                  </Box>
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 2, lineHeight: 1.5 }}>
                    {func.description}
                  </Typography>
                  
                  <Box sx={{ display: 'flex', gap: 1 }}>
                    {onAskAI && (
                      <Button
                        variant="contained"
                        size="small"
                        fullWidth
                        onClick={() => {
                          const explanation = getExplanation(func.name);
                          // Build a single inline call expression — no fenced code, no print(), no result =
                          const inlineCall = explanation?.inlineExample
                            || `${func.name}(${func.params})`;
                          const outputHint = explanation?.tested && explanation?.sampleOutput
                            ? ` Verified result: \`${explanation.sampleOutput}\`.`
                            : '';
                          onAskAI(
                            func.name,
                            `Explain the \`${func.name}()\` DSL function in plain English using the FUNCTION-DEMO TEMPLATE.\n\n` +
                            `Parameters: ${func.params}\n` +
                            `Description: ${func.description}\n` +
                            `Use this exact inline example (do NOT change it, do NOT wrap it in a fenced code block): \`${inlineCall}\`.${outputHint}\n\n` +
                            `Required reply structure:\n` +
                            `- One-sentence description of what \`${func.name}()\` does.\n` +
                            `- A bold **Example:** label followed by the inline call above (single backticks only).\n` +
                            `- A bold **Computation:** label followed by a bullet list: each argument value with a short meaning, the formula substitution in plain English, and the resulting value as inline code.\n` +
                            `- A bold **When to use it in the Rule Builder:** label followed by one short tip referencing the right step (Parameters / Schedule / Iteration / Conditional / Transaction).\n\n` +
                            `HARD RULES:\n` +
                            `- NEVER use a fenced code block (no \`\`\`dsl, no \`\`\`python, no \`\`\` of any kind).\n` +
                            `- NEVER include \`print(...)\`, \`result = ...\`, or \`##\` comment lines.\n` +
                            `- NEVER write a custom multi-step DSL rule.\n` +
                            `- NEVER call \`createTransaction()\`.\n` +
                            `- NEVER tell the user to paste DSL into the editor.`
                          );
                          onClose();
                        }}
                        startIcon={<Sparkles size={14} />}
                        data-testid={`ask-ai-${func.name}`}
                        sx={{
                          bgcolor: '#14213D',
                          color: '#FFFFFF',
                          '&:hover': { bgcolor: '#1D3557' }
                        }}
                      >
                        Ask AI
                      </Button>
                    )}
                  </Box>
                </CardContent>
              </Card>
            ))}
          </Box>

          {filteredFunctions.length === 0 && (
            <Box sx={{ textAlign: 'center', py: 6 }}>
              <Search size={48} color="#CED4DA" style={{ marginBottom: 16 }} />
              <Typography variant="h5" sx={{ mb: 1 }}>No formulas found</Typography>
              <Typography variant="body2" color="text.secondary">
                Try a different search term or category
              </Typography>
            </Box>
          )}
        </Box>

        <Box sx={{ pt: 2, borderTop: '1px solid #E9ECEF', bgcolor: '#F8F9FA', px: 2, py: 1.5, mx: -3, mb: -3, mt: 2 }}>
          <Typography variant="caption" color="text.secondary" sx={{ textAlign: 'center', display: 'block' }}>
            Use "Build Formula" to create your own custom formulas
          </Typography>
        </Box>
      </DialogContent>
    </Dialog>
  );
};

export default FunctionBrowser;