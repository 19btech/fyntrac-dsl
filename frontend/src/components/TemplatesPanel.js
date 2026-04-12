import React, { useState } from "react";
import { Card, CardContent, Button, IconButton, Box, Typography, Chip, Tooltip } from '@mui/material';
import AppDialog, { useAppDialog } from './AppDialog';
import { Play, FileText, Clock, Trash2, Rocket } from "lucide-react";

const TemplatesPanel = ({ templates, onLoadTemplate, onRunTemplate, onDeleteTemplate, onDeployTemplate, selectedEvent }) => {
  const [deletingIds, setDeletingIds] = useState(new Set());
  const [deployingIds, setDeployingIds] = useState(new Set());
  const { confirmProps, openConfirm } = useAppDialog();
  
  return (
    <Box sx={{ p: 3, bgcolor: '#F8F9FA', minHeight: '100%' }} data-testid="templates-panel">
      <Box sx={{ mb: 3 }}>
        <Typography variant="h3" sx={{ mb: 0.5 }}>Saved Templates</Typography>
        <Typography variant="body2" color="text.secondary">Load and run your saved calculation templates</Typography>
      </Box>

      {templates.length === 0 ? (
        <Card sx={{ textAlign: 'center', py: 6 }}>
          <CardContent>
            <FileText size={48} color="#CED4DA" style={{ marginBottom: 16 }} />
            <Typography variant="h5" sx={{ mb: 1 }}>No Templates Yet</Typography>
            <Typography variant="body2" color="text.secondary">
              Save your calculations to create reusable templates
            </Typography>
          </CardContent>
        </Card>
      ) : (
        <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(2, 1fr)', lg: 'repeat(3, 1fr)' }, gap: 2 }}>
          {templates.map((template) => (
            <Card 
              key={template.id}
              data-testid={`template-${template.id}`}
            >
              <CardContent sx={{ p: 2.5 }}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                  <Box sx={{ flex: 1 }}>
                    <Typography variant="h6" sx={{ mb: 0.5 }}>{template.name}</Typography>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                      <Clock size={12} color="#6C757D" />
                      <Typography variant="caption" color="text.secondary">
                        {new Date(template.created_at).toLocaleDateString()}
                      </Typography>
                    </Box>
                  </Box>
                  <Tooltip title="Delete">
                    <span>
                      <IconButton
                    size="small"
                    onClick={(e) => {
                      e.stopPropagation();
                      openConfirm({
                        title: "Delete Template",
                        message: `Delete template "${template.name}"?`,
                        confirmLabel: "Delete",
                        confirmColor: "error",
                        onConfirm: async () => {
                          try {
                            setDeletingIds(prev => new Set(prev).add(template.id));
                            await onDeleteTemplate(template.id, template.name);
                          } catch (err) {
                            console.error('Error deleting template', err);
                          } finally {
                            setDeletingIds(prev => {
                              const copy = new Set(prev);
                              copy.delete(template.id);
                              return copy;
                            });
                          }
                        }
                      });
                    }}
                    sx={{ color: '#6C757D' }}
                    data-testid={`delete-template-${template.id}`}
                    disabled={deletingIds.has(template.id)}
                  >
                    <Trash2 size={16} />
                      </IconButton>
                    </span>
                  </Tooltip>
                  <Tooltip title="Publish">
                    <span>
                      <IconButton
                        size="small"
                        onClick={(e) => {
                          e.stopPropagation();
                          openConfirm({
                            title: "Deploy Template",
                            message: `Deploy template "${template.name}" as model?`,
                            confirmLabel: "Deploy",
                            onConfirm: async () => {
                              try {
                                setDeployingIds(prev => new Set(prev).add(template.id));
                                if (typeof onDeployTemplate === 'function') {
                                  await onDeployTemplate(template.id, template.name);
                                } else {
                                  // fallback: simulate deploy
                                  await new Promise(res => setTimeout(res, 800));
                                }
                              } catch (err) {
                                console.error('Error deploying template', err);
                              } finally {
                                setDeployingIds(prev => {
                                  const copy = new Set(prev);
                                  copy.delete(template.id);
                                  return copy;
                                });
                              }
                            }
                          });
                        }}
                        sx={{
                          color: deployingIds.has(template.id) ? '#FFC107' : '#5B5FED',
                          transition: 'transform 200ms ease, box-shadow 200ms ease',
                          '&:hover': { transform: 'translateY(-3px) rotate(-8deg)', boxShadow: '0 6px 18px rgba(91,95,237,0.18)' }
                        }}
                        data-testid={`deploy-template-${template.id}`}
                        disabled={deployingIds.has(template.id)}
                      >
                        <Rocket size={16} />
                      </IconButton>
                    </span>
                  </Tooltip>
                </Box>

                <Box sx={{ 
                  bgcolor: '#212529', 
                  borderRadius: 1, 
                  p: 1.5, 
                  mb: 2,
                  maxHeight: 120,
                  overflow: 'auto',
                  fontFamily: 'monospace',
                  fontSize: '0.75rem',
                  color: '#F8F9FA',
                  lineHeight: 1.5
                }}>
                  {template.dsl_code}
                </Box>

                <Box sx={{ display: 'flex', gap: 1 }}>
                  <Button 
                    variant="outlined" 
                    size="small" 
                    onClick={() => onLoadTemplate(template)}
                    sx={{ flex: 1 }}
                    startIcon={<FileText size={16} />}
                    data-testid={`load-template-${template.id}`}
                  >
                    Load
                  </Button>
                  <Button 
                    variant="contained"
                    size="small" 
                    onClick={() => onRunTemplate(template.id)}
                    disabled={!selectedEvent}
                    sx={{ flex: 1 }}
                    startIcon={<Play size={16} />}
                    data-testid={`run-template-${template.id}`}
                  >
                    Run
                  </Button>
                </Box>
              </CardContent>
            </Card>
          ))}
        </Box>
      )}

      <AppDialog {...confirmProps} />
    </Box>
  );
};

export default TemplatesPanel;
