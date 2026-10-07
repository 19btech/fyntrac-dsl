import { API } from "../config";

/**
 * How the editor's code relates to the saved workspace rules.
 *   'same'    - editor holds exactly the saved rules.
 *   'unsaved' - editor was loaded from the saved rules and then edited; the
 *               saved rules have not moved since. Running the editor is the
 *               intended "test my unsaved edits" case.
 *   'stale'   - the saved rules changed after the editor loaded them.
 *   'unknown' - editor differs and we don't know what it was loaded from
 *               (a template, the builder, a previous session).
 * If the saved rules can't be fetched, answers 'unsaved' so a network blip
 * never blocks a run -- the run then behaves exactly as it always did.
 */
export async function classifyEditorCode(editorCode, editorBaseCode, fetchImpl = fetch) {
  let saved;
  try {
    const res = await fetchImpl(`${API}/combined-code`);
    if (!res.ok) return 'unsaved';
    saved = (await res.json())?.code ?? '';
  } catch {
    return 'unsaved';
  }
  if ((editorCode || '') === saved) return 'same';
  if (editorBaseCode === null || editorBaseCode === undefined) return 'unknown';
  return editorBaseCode === saved ? 'unsaved' : 'stale';
}
