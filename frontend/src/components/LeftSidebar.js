import React from "react";
import { Box, Button, Card, Collapse, List, ListItemButton, ListItemIcon, ListItemText, Tooltip, IconButton } from '@mui/material';
import { FileText, RefreshCw, ChevronDown, ChevronRight, ChevronLeft, Upload, Eye, PanelLeftClose, PanelLeftOpen } from "lucide-react";
import { useToast } from "./ToastProvider";
import ImportEventsModal from "./ImportEventsModal";

const formatDataType = (dt) => {
  const map = { string: 'text', decimal: 'number', integer: 'whole number', int: 'whole number', boolean: 'yes/no', bool: 'yes/no', date: 'date' };
  return map[(dt || '').toLowerCase()] || dt;
};
const formatEventTable = (t) => t;
const formatEventType = (t) => t;

const LeftSidebar = ({ events, selectedEvent, onEventSelect, onDownloadEvents, onImportSuccess, onViewEventData, collapsed = false, onToggleCollapsed }) => {
  const [expandedEvent, setExpandedEvent] = React.useState(null);
  const [importModalOpen, setImportModalOpen] = React.useState(false);
  const toast = useToast();

  const toggleExpand = (eventName) => {
    setExpandedEvent(prev => (prev === eventName ? null : eventName));
  };

  if (collapsed) {
    return (
      <Box
        sx={{
          width: 44,
          bgcolor: '#FFFFFF',
          borderRight: '1px solid #E9ECEF',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          height: '100vh',
          py: 1,
          transition: 'width 200ms ease',
        }}
        data-testid="left-sidebar-collapsed"
      >
        <Tooltip title="Expand events panel" placement="right">
          <IconButton size="small" onClick={onToggleCollapsed} aria-label="Expand sidebar">
            <PanelLeftOpen size={18} />
          </IconButton>
        </Tooltip>
        <Tooltip title={`${events?.length || 0} events`} placement="right">
          <Box sx={{ mt: 1, color: '#6C757D' }}>
            <FileText size={18} />
          </Box>
        </Tooltip>
      </Box>
    );
  }

  return (
    <Box 
      sx={{ 
        width: 280, 
        bgcolor: '#FFFFFF', 
        borderRight: '1px solid #E9ECEF', 
        display: 'flex', 
        flexDirection: 'column',
        height: '100vh',
        transition: 'width 200ms ease',
        position: 'relative',
      }} 
      data-testid="left-sidebar"
    >
      {onToggleCollapsed && (
        <Tooltip title="Collapse events panel" placement="right">
          <IconButton
            size="small"
            onClick={onToggleCollapsed}
            aria-label="Collapse sidebar"
            sx={{
              position: 'absolute',
              top: 8,
              right: 4,
              zIndex: 2,
              bgcolor: 'rgba(255,255,255,0.85)',
              '&:hover': { bgcolor: '#F1F3F5' },
            }}
          >
            <PanelLeftClose size={16} />
          </IconButton>
        </Tooltip>
      )}
      <Box sx={{ p: 3, borderBottom: '1px solid #E9ECEF', display: 'flex', justifyContent: 'center' }}>
        <img
          src={process.env.PUBLIC_URL + '/logo.png'}
          alt="Fyntrac"
          style={{ height: 40, objectFit: 'contain' }}
          data-testid="sidebar-logo"
          onError={(e) => { e.currentTarget.onerror = null; e.currentTarget.src = 'https://customer-assets.emergentagent.com/job_code-finance-2/artifacts/hdj19r3w_Fyntrac%20%28600%20x%20400%20px%29%20%284%29.png'; }}
        />
      </Box>

      <Box sx={{ flex: 1, overflowY: 'auto', p: 2 }}>
        <Box sx={{ mb: 3 }}>
            <Card sx={{ bgcolor: '#EEF0FE', border: '1px solid #D4D6FA', p: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5 }}>
              <RefreshCw size={16} color="#5B5FED" />
              <Box component="span" sx={{ fontSize: '0.8125rem', fontWeight: 600, color: '#212529' }}>
                Event Setup
              </Box>
            </Box>
            <Button
              variant="contained"
              size="small"
              onClick={() => setImportModalOpen(true)}
              fullWidth
              startIcon={<Upload size={14} color="#FFFFFF" />}
              data-testid="import-events-button"
              sx={{
                fontSize: '0.8125rem',
                fontWeight: 600,
                bgcolor: '#14213d',
                borderColor: '#14213d',
                color: '#FFFFFF',
                boxShadow: '0 2px 8px rgba(20, 33, 61, 0.3)',
                transition: 'all 0.15s ease',
                '&:hover': {
                  bgcolor: '#1D3557',
                  boxShadow: '0 6px 20px rgba(20, 33, 61, 0.4)',
                  transform: 'translateY(-2px)'
                },
                '&:active': {
                  transform: 'translateY(0)',
                  boxShadow: '0 2px 8px rgba(20, 33, 61, 0.3)'
                },
              }}
            >
              Import
            </Button>
          </Card>
        </Box>

        <Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.5, px: 1 }}>
            <FileText size={16} color="#5B5FED" />
            <Box component="span" sx={{ fontSize: '0.8125rem', fontWeight: 600, color: '#495057', flex: 1 }}>Events</Box>
            {events.length > 0 && onViewEventData && (
              <Tooltip title="View event data" arrow>
                <IconButton
                  size="small"
                  onClick={onViewEventData}
                  data-testid="view-event-data-button"
                  sx={{ p: 0.25, color: '#5B5FED', '&:hover': { bgcolor: 'rgba(91, 95, 237, 0.08)' } }}
                >
                  <Eye size={14} />
                </IconButton>
              </Tooltip>
            )}
          </Box>
          {events.length === 0 ? (
            <Box sx={{ px: 1, py: 2 }}>
              <Box component="p" sx={{ fontSize: '0.75rem', color: '#6C757D', m: 0 }}>No events uploaded yet</Box>
            </Box>
          ) : (
            <List sx={{ p: 0 }}>
              {events.map((event) => {
                const isExpanded = expandedEvent === event.event_name;
                const isSelected = selectedEvent === event.event_name;
                return (
                  <Box key={event.id} sx={{ mb: 0.5 }}>
                    <ListItemButton
                      onClick={() => { toggleExpand(event.event_name); onEventSelect && onEventSelect(event.event_name); }}
                      selected={isSelected}
                      data-testid={`event-${event.event_name}`}
                      sx={{
                        borderRadius: 1,
                        py: 1,
                        px: 1.5,
                        '&.Mui-selected': {
                          bgcolor: '#EEF0FE',
                          '&:hover': {
                            bgcolor: '#E0E2FD',
                          },
                        },
                      }}
                    >
                      <ListItemIcon sx={{ minWidth: 32 }}>
                        {isExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                      </ListItemIcon>
                      <ListItemText 
                        primary={event.event_name} 
                        primaryTypographyProps={{ 
                          fontSize: '0.875rem', 
                          fontWeight: isSelected ? 600 : 500,
                          color: '#495057'
                        }}
                      />
                    </ListItemButton>

                    <Collapse in={isExpanded} timeout="auto">
                      <Box sx={{ pl: 5, pr: 1.5, py: 1.5 }}>
                        <Box sx={{ fontSize: '0.75rem', color: '#6C757D', lineHeight: 1.6 }}>
                          {(() => {
                            const evtType = (event.eventType || event.event_type || '').toString().toLowerCase();
                            const evtTable = (event.eventTable || event.event_table || 'standard').toString().toLowerCase();
                            // Hide standard system fields for custom reference events
                            const isCustomReference = evtTable === 'custom' && evtType === 'reference';
                            const showStandard = !isCustomReference;
                            return (
                              <>
                                {/* Show eventTable tag */}
                                <Box sx={{ mb: 1, display: 'flex', gap: 0.5 }}>
                                  <Box component="span" sx={{
                                    fontSize: '0.625rem', fontWeight: 600, px: 0.75, py: 0.25, borderRadius: 0.5,
                                    bgcolor: evtTable === 'standard' ? '#E8F5E9' : '#FFF3E0',
                                    color: evtTable === 'standard' ? '#2E7D32' : '#E65100'
                                  }}>
                                    {formatEventTable(evtTable)}
                                  </Box>
                                  <Box component="span" sx={{
                                    fontSize: '0.625rem', fontWeight: 600, px: 0.75, py: 0.25, borderRadius: 0.5,
                                    bgcolor: evtType === 'activity' ? '#E3F2FD' : '#F3E5F5',
                                    color: evtType === 'activity' ? '#1565C0' : '#7B1FA2'
                                  }}>
                                    {formatEventType(evtType)}
                                  </Box>
                                </Box>

                                {showStandard && (
                                  <>
                                    <Box sx={{ mb: 0.5 }}>
                                      <Box component="span" sx={{ fontFamily: 'monospace', color: '#5B5FED', fontWeight: 500 }}>• Posting Date</Box>
                                      <Box component="span" sx={{ color: '#ADB5BD', ml: 0.5 }}>(date)</Box>
                                    </Box>
                                    <Box sx={{ mb: 0.5 }}>
                                      <Box component="span" sx={{ fontFamily: 'monospace', color: '#5B5FED', fontWeight: 500 }}>• Effective Date</Box>
                                      <Box component="span" sx={{ color: '#ADB5BD', ml: 0.5 }}>(date)</Box>
                                    </Box>
                                    <Box sx={{ mb: 0.5 }}>
                                      <Box component="span" sx={{ fontFamily: 'monospace', color: '#5B5FED', fontWeight: 500 }}>• Sub-Instrument ID</Box>
                                      <Box component="span" sx={{ color: '#ADB5BD', ml: 0.5 }}>(text)</Box>
                                    </Box>
                                  </>
                                )}

                                {event.fields.map((field, idx) => (
                                  <Box key={idx} sx={{ mb: 0.5, display: 'flex', justifyContent: 'space-between' }}>
                                    <Box component="span" sx={{ fontFamily: 'monospace', fontWeight: 500 }}>{field.name}</Box>
                                    <Box component="span" sx={{ color: '#ADB5BD' }}>({formatDataType(field.datatype)})</Box>
                                  </Box>
                                ))}
                              </>
                            );
                          })()}
                        </Box>
                      </Box>
                    </Collapse>
                  </Box>
                );
              })}
            </List>
          )}
        </Box>
      </Box>
      <ImportEventsModal
        open={importModalOpen}
        onClose={() => setImportModalOpen(false)}
        onSuccess={({ slot, data }) => {
          try {
            if (slot === 'transactions') {
              toast.success(`${data.count} transaction type(s) loaded.`);
            } else if (slot === 'event_configurations') {
              const names = (data.names || []).join(', ');
              toast.success(`${data.count} event definition(s) loaded${names ? `: ${names}` : ''}.`);
              localStorage.setItem('uploadedEventFileName', 'EventConfigurations.json');
              window.dispatchEvent(new CustomEvent('dsl-event-def-loaded', { detail: { filename: 'EventConfigurations.json' } }));
            } else if (slot === 'event_data') {
              const events = (data.events || []).join(', ');
              toast.success(`${data.total_rows} row(s) loaded${events ? ` across ${events}` : ''}.`);
              localStorage.setItem('uploadedExcelFileName', 'EventData.xlsx');
              window.dispatchEvent(new CustomEvent('dsl-event-data-imported', { detail: { filename: 'EventData.xlsx' } }));
            }
          } catch (e) {}
          if (onImportSuccess) onImportSuccess();
        }}
      />
    </Box>
  );
};

export default LeftSidebar;