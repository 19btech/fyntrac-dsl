import React, { useState, useMemo } from "react";
import { Dialog, DialogContent, DialogTitle, Card, CardContent, Button, TextField, IconButton, InputAdornment, Chip, Box, Typography, Tooltip } from '@mui/material';
import { Search, BookOpen, Copy, X, Sparkles } from "lucide-react";
import { useToast } from "./ToastProvider";
import { getExplanation } from "../agent/testing/explanationStore";

const FunctionBrowser = ({ dslFunctions, onInsertFunction, onClose, onAskAI }) => {
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
              <Typography variant="h4">DSL Function Browser</Typography>
              <Typography variant="body2" color="text.secondary">
                {dslFunctions.length} functions available
                {customCount > 0 && (
                  <Box component="span" sx={{ ml: 1, color: '#7C3AED' }}>
                    ({customCount} custom)
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
            placeholder="Search functions by name, description, or parameters..."
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
            Showing {filteredFunctions.length} of {dslFunctions.length} functions
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
                          label="Custom"
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
                    <Button
                      variant="outlined"
                      size="small"
                      sx={{ flex: 1 }}
                      onClick={() => {
                        onInsertFunction(`${func.name}(${func.params})`);
                        toast.success(`Inserted: ${func.name}()`);
                      }}
                      data-testid={`insert-${func.name}`}
                    >
                      Insert into Editor
                    </Button>
                    {onAskAI && (
                      <Button
                        variant="contained"
                        size="small"
                        onClick={() => {
                          const explanation = getExplanation(func.name);
                          const exampleCode = explanation?.dslExample
                            ? explanation.dslExample
                            : `## ${func.name} example\nresult = ${func.name}(${func.params})\nprint(result)`;
                          const outputHint = explanation?.tested && explanation?.sampleOutput
                            ? `\nExpected output: ${explanation.sampleOutput}`
                            : '';
                          onAskAI(
                            func.name,
                            `Show me a working example of the ${func.name}() DSL function.\n` +
                            `Parameters: ${func.params}\n` +
                            `Description: ${func.description}\n\n` +
                            `Here is a verified working example (standalone mode, no event fields):\n` +
                            `\`\`\`dsl\n${exampleCode}\n\`\`\`${outputHint}\n\n` +
                            `Please explain what this code does and generate a clear standalone DSL example ` +
                            `using only literal values (no EVENT.field references). ` +
                            `Use print() to display the result.`
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
              <Typography variant="h5" sx={{ mb: 1 }}>No functions found</Typography>
              <Typography variant="body2" color="text.secondary">
                Try adjusting your search or filter criteria
              </Typography>
            </Box>
          )}
        </Box>

        <Box sx={{ pt: 2, borderTop: '1px solid #E9ECEF', bgcolor: '#F8F9FA', px: 2, py: 1.5, mx: -3, mb: -3, mt: 2 }}>
          <Typography variant="caption" color="text.secondary" sx={{ textAlign: 'center', display: 'block' }}>
            Use "Build Function" to create custom DSL functions
          </Typography>
        </Box>
      </DialogContent>
    </Dialog>
  );
};

export default FunctionBrowser;