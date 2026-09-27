import React, { useMemo } from "react";
import { DataGrid } from "@mui/x-data-grid";

/**
 * The one table in the application.
 *
 * Wraps MUI's DataGrid so every table looks and behaves the same: identical
 * header treatment, zebra striping, density and column menu (sort, filter,
 * hide, manage columns). Converting a table means supplying rows and columns;
 * nothing else should need restating at the call site.
 *
 * Two shapes:
 *   - fill (default) — stretches to its container, paginates, virtualises.
 *     For tables of records people scan, filter and sort.
 *   - autoHeight — grows to fit its rows, no footer. For the short fixed
 *     summaries where a scrolling viewport and a pager would be noise.
 *
 * Rows need a stable id. Most of our data has no natural key — the same
 * instrument, date and type can legitimately repeat — so an index is assigned
 * when none is supplied.
 */

/* Brand tokens — mirrors LivePreview.js so tables match the rest of the app. */
export const T = {
  brand: '#5B5FED', brandSoft: '#EEF0FE',
  ink: '#14213D', body: '#495057', muted: '#6C757D',
  border: '#E9ECEF', surface: '#FFFFFF',
  danger: '#DC2626', zebra: '#FAFBFF',
};

/* Sub-instrument 10 must sort after 9, not after 1. The grid compares strings
 * lexicographically by default, and these ids are numeric more often than not.
 * Exported so any column of numeric-ish strings can opt in. */
export const numericAwareComparator = (a, b) => {
  const an = Number(a), bn = Number(b);
  const aNum = Number.isFinite(an), bNum = Number.isFinite(bn);
  if (aNum && bNum) return an - bn;
  if (aNum !== bNum) return aNum ? -1 : 1;
  return String(a ?? '').localeCompare(String(b ?? ''));
};

const baseSx = {
  bgcolor: T.surface,
  border: `1px solid ${T.border}`,
  borderRadius: 1.5,
  fontSize: '0.75rem',
  '& .MuiDataGrid-columnHeader': { bgcolor: T.brandSoft },
  '& .MuiDataGrid-columnHeaderTitle': { fontWeight: 700, color: T.ink },
  '& .MuiDataGrid-row:nth-of-type(odd)': { bgcolor: T.zebra },
  '& .MuiDataGrid-row:hover': { bgcolor: T.brandSoft },
  '& .MuiDataGrid-cell': { color: T.body },
  '& .MuiDataGrid-cell:focus, & .MuiDataGrid-cell:focus-within': { outline: 'none' },
  '& .MuiDataGrid-columnHeader:focus, & .MuiDataGrid-columnHeader:focus-within': { outline: 'none' },
  // Shared cell classes so call sites can style without restating the CSS.
  '& .cell-strong': { fontWeight: 600, color: T.ink },
  '& .cell-mono': { fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace' },
  '& .cell-num': {
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontWeight: 600, color: T.ink,
  },
  '& .cell-num.neg': { color: T.danger },
};

/* The MIT DataGrid refuses a pageSize above 100 — it throws rather than
 * degrading, which takes the whole screen down via the error boundary. Clamp
 * here so no call site can reintroduce that. */
export const MAX_PAGE_SIZE = 100;

export default function DataTable({
  rows = [],
  columns,
  autoHeight = false,
  pageSize = MAX_PAGE_SIZE,
  pageSizeOptions = [25, 50, 100],
  emptyLabel = 'No rows',
  sx,
  ...rest
}) {
  // Assign an id only when the caller has not. Doing it here keeps every call
  // site from repeating the same map.
  const withIds = useMemo(() => {
    if (!rows.length || rows[0]?.id !== undefined) return rows;
    return rows.map((r, i) => ({ ...r, id: i }));
  }, [rows]);

  // Server-paged callers pass paginationModel/pageSizeOptions through `rest`,
  // which is spread last and would otherwise defeat the clamp. Pull them out
  // and bound them here so the cap holds however a page size arrives.
  const { paginationModel, pageSizeOptions: restOptions, ...other } = rest;

  const clamp = (n) => Math.min(n, MAX_PAGE_SIZE);
  const bound = (opts) => {
    const kept = (opts || []).map(clamp).filter((n, i, a) => a.indexOf(n) === i);
    return kept.length ? kept : [MAX_PAGE_SIZE];
  };

  const options = bound(restOptions || pageSizeOptions);
  const paging = autoHeight
    ? { hideFooter: true }
    : {
        pageSizeOptions: options,
        ...(paginationModel
          ? { paginationModel: { ...paginationModel, pageSize: clamp(paginationModel.pageSize) } }
          : { initialState: { pagination: { paginationModel: { pageSize: clamp(pageSize) } } } }),
      };

  return (
    <DataGrid
      rows={withIds}
      columns={columns}
      density="compact"
      autoHeight={autoHeight}
      disableRowSelectionOnClick
      localeText={{ noRowsLabel: emptyLabel }}
      sx={{ ...baseSx, ...(autoHeight ? {} : { height: '100%' }), ...sx }}
      {...paging}
      {...other}
    />
  );
}
