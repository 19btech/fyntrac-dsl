import React, { useState, useMemo } from "react";
import { Card, CardContent, Button, Dialog, DialogTitle, DialogContent, IconButton, TextField, Select, MenuItem, Box, Typography, Chip, Table, TableHead, TableBody, TableRow, TableCell, Pagination, Tooltip } from '@mui/material';
import { Download, FileText, Clock, Trash2, TrendingUp, X as CloseIcon } from "lucide-react";

const PageSizeOptions = [10, 25, 50, 100];

const TransactionReports = ({ reports, onDownloadReport, onDeleteReport }) => {
  const [selectedReport, setSelectedReport] = useState(null);
  const [pageSize, setPageSize] = useState(10);
  const [pageIndex, setPageIndex] = useState(0);
  const [filterText, setFilterText] = useState("");
  const [modalOpen, setModalOpen] = useState(false);

  const displayReports = useMemo(() => {
    const map = new Map();
    (reports || []).forEach(r => map.set(r.template_name, r));
    return Array.from(map.values());
  }, [reports]);

  const openReport = (report) => {
    setSelectedReport(report);
    setPageIndex(0);
    setFilterText("");
    setModalOpen(true);
  };

  const filteredTransactions = useMemo(() => {
    if (!selectedReport || !selectedReport.transactions) return [];
    const txt = filterText.trim().toLowerCase();
    if (!txt) return selectedReport.transactions;
    return selectedReport.transactions.filter(tx => {
      return (
        (tx.instrumentid && String(tx.instrumentid).toLowerCase().includes(txt)) ||
        (tx.subinstrumentid && String(tx.subinstrumentid).toLowerCase().includes(txt)) ||
        (tx.transactiontype && String(tx.transactiontype).toLowerCase().includes(txt))
      );
    });
  }, [selectedReport, filterText]);

  const pageCount = Math.max(1, Math.ceil(filteredTransactions.length / pageSize));
  const pageRows = filteredTransactions.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);

  return (
    <Box sx={{ p: 3, bgcolor: '#F8F9FA', minHeight: '100%', fontFamily: 'Inter, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial' }} data-testid="transaction-reports">
      <Box sx={{ mb: 3 }}>
        <Typography variant="h3" sx={{ mb: 0.5 }}>Transaction Report</Typography>
        <Typography variant="body2" color="text.secondary">
          View and download generated transaction reports
        </Typography>
      </Box>

      {(!displayReports || displayReports.length === 0) ? (
        <Card sx={{ textAlign: 'center', py: 6 }}>
          <CardContent>
            <TrendingUp size={48} color="#CED4DA" style={{ marginBottom: 16 }} />
            <Typography variant="h5" sx={{ mb: 1 }}>No Transaction Report</Typography>
            <Typography variant="body2" color="text.secondary">
              Run a template to generate transaction reports
            </Typography>
          </CardContent>
        </Card>
      ) : (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5 }}>
          {displayReports.map((report) => (
            <Card key={report.id || report.template_name} data-testid={`report-${report.id}`}>
              <CardContent sx={{ p: 3 }}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2.5 }}>
                  <Box>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                      <FileText size={20} color="#5B5FED" />
                      <Typography variant="h5">{report.template_name}</Typography>
                    </Box>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                      <Typography variant="body2" color="text.secondary">
                        Event: <Box component="span" sx={{ fontFamily: 'monospace', color: '#495057' }}>{report.event_name}</Box>
                      </Typography>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                        <Clock size={12} color="#6C757D" />
                        <Typography variant="caption" color="text.secondary">
                          {report.executed_at ? new Date(report.executed_at).toLocaleString() : ''}
                        </Typography>
                      </Box>
                    </Box>
                  </Box>
                  <Box sx={{ display: 'flex', gap: 1 }}>
                    <Button 
                      size="small" 
                      variant="outlined"
                      onClick={() => openReport(report)} 
                      data-testid={`open-report-${report.id}`}
                    >
                      View Details
                    </Button>
                    <Tooltip title="Delete">
                      <span>
                        <IconButton
                          size="small"
                          onClick={() => onDeleteReport(report.id, report.template_name)}
                          sx={{ color: '#6C757D' }}
                          data-testid={`delete-report-${report.id}`}
                        >
                          <Trash2 size={16} />
                        </IconButton>
                      </span>
                    </Tooltip>
                  </Box>
                </Box>

                <Card sx={{ bgcolor: '#F8F9FA', border: '1px solid #E9ECEF' }}>
                  <CardContent sx={{ p: 2.5 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                      <Box>
                        <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>Generated Transactions</Typography>
                        <Typography variant="caption" color="text.secondary">Click "View Details" to see full report</Typography>
                      </Box>
                      <Chip 
                        label={`${report.transactions.length} transactions`}
                        sx={{ 
                          bgcolor: '#D4EDDA',
                          color: '#155724',
                          fontWeight: 600,
                          fontSize: '0.875rem'
                        }}
                      />
                    </Box>

                    {report.transactions.length > 0 && (
                      <Box sx={{ overflowX: 'auto' }}>
                        <Table size="small">
                          <TableHead>
                            <TableRow>
                              <TableCell>Posting Date</TableCell>
                              <TableCell>Instrument</TableCell>
                              <TableCell>Type</TableCell>
                              <TableCell align="right">Amount</TableCell>
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {report.transactions.slice(0, 5).map((tx, idx) => (
                              <TableRow key={idx} hover>
                                <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{tx.postingdate}</TableCell>
                                <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.8125rem' }}>{tx.instrumentid}</TableCell>
                                <TableCell sx={{ fontSize: '0.8125rem' }}>{tx.transactiontype}</TableCell>
                                <TableCell align="right" sx={{ fontFamily: 'monospace', fontSize: '0.8125rem', fontWeight: 600 }}>
                                  {parseFloat(tx.amount).toFixed(2)}
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                        {report.transactions.length > 5 && (
                          <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mt: 1, textAlign: 'center' }}>
                            + {report.transactions.length - 5} more transactions
                          </Typography>
                        )}
                      </Box>
                    )}
                  </CardContent>
                </Card>

                <Box sx={{ mt: 2 }}>
                  <Button 
                    variant="contained"
                    size="small" 
                    onClick={() => onDownloadReport(report.id)}
                    startIcon={<Download size={16} />}
                    data-testid={`download-report-${report.id}`}
                  >
                    Download CSV
                  </Button>
                </Box>
              </CardContent>
            </Card>
          ))}
        </Box>
      )}

      <Dialog 
        open={modalOpen} 
        onClose={() => setModalOpen(false)} 
        maxWidth="lg" 
        fullWidth
      >
        <DialogTitle>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="h4">{selectedReport ? `${selectedReport.template_name} — Transactions` : 'Transactions'}</Typography>
            <IconButton onClick={() => setModalOpen(false)} size="small">
              <CloseIcon size={20} />
            </IconButton>
          </Box>
        </DialogTitle>
        <DialogContent>
          <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
            <TextField 
              value={filterText} 
              onChange={e => setFilterText(e.target.value)} 
              placeholder="Filter transactions..." 
              size="small"
              fullWidth
            />
          </Box>

          <Box sx={{ overflowX: 'auto' }}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Posting Date</TableCell>
                  <TableCell>Instrument ID</TableCell>
                  <TableCell>Sub-Instrument</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell align="right">Amount</TableCell>
                  <TableCell>Effective Date</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {pageRows.map((tx, i) => (
                  <TableRow key={i} hover>
                    <TableCell sx={{ fontFamily: 'monospace' }}>{tx.postingdate}</TableCell>
                    <TableCell sx={{ fontFamily: 'monospace' }}>{tx.instrumentid}</TableCell>
                    <TableCell sx={{ fontFamily: 'monospace' }}>{tx.subinstrumentid || '1'}</TableCell>
                    <TableCell>{tx.transactiontype}</TableCell>
                    <TableCell align="right" sx={{ fontFamily: 'monospace', fontWeight: 600 }}>
                      {parseFloat(tx.amount).toFixed(2)}
                    </TableCell>
                    <TableCell sx={{ fontFamily: 'monospace' }}>{tx.effectivedate}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Box>

          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 3 }}>
            <Typography variant="body2" color="text.secondary">
              Showing {filteredTransactions.length} transactions — page {pageIndex + 1} of {pageCount}
            </Typography>
            <Box sx={{ display: 'flex', gap: 2, alignItems: 'center' }}>
              <Select 
                value={pageSize} 
                onChange={e => { setPageSize(parseInt(e.target.value, 10)); setPageIndex(0); }} 
                size="small"
                sx={{ minWidth: 120 }}
              >
                {PageSizeOptions.map(s => <MenuItem key={s} value={s}>{s} per page</MenuItem>)}
              </Select>
              <Pagination
                count={pageCount}
                page={pageIndex + 1}
                onChange={(e, value) => setPageIndex(Math.max(0, value - 1))}
                color="primary"
                siblingCount={1}
                boundaryCount={1}
              />
            </Box>
          </Box>
        </DialogContent>
      </Dialog>
    </Box>
  );
};

export default TransactionReports;
