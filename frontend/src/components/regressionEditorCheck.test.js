import { classifyEditorCode } from './regressionEditorCheck';

// A "Workspace rules" regression run sends the editor's code. When the saved
// rules change underneath the editor (the agent saved a rule), that code is
// stale and the run silently tests old logic -- the UI showed pre-fix numbers
// while the agent's run of the same case passed.
const savedIs = (code) => async () => ({ ok: true, json: async () => ({ code }) });

test('editor matching the saved rules is fine', async () => {
  expect(await classifyEditorCode('A', 'A', savedIs('A'))).toBe('same');
});

test('unsaved edits on top of current saved rules run as before', async () => {
  expect(await classifyEditorCode('A + my edit', 'A', savedIs('A'))).toBe('unsaved');
});

test('saved rules changed after the editor loaded them is stale', async () => {
  expect(await classifyEditorCode('A', 'A', savedIs('B'))).toBe('stale');
});

test('edited editor is still stale when saved rules also moved', async () => {
  expect(await classifyEditorCode('A + my edit', 'A', savedIs('B'))).toBe('stale');
});

test('unknown origin that differs from saved is flagged', async () => {
  expect(await classifyEditorCode('from localStorage', null, savedIs('B'))).toBe('unknown');
});

test('unknown origin that happens to match saved is fine', async () => {
  expect(await classifyEditorCode('B', null, savedIs('B'))).toBe('same');
});

test('a failed check never blocks a run', async () => {
  const down = async () => { throw new Error('network'); };
  expect(await classifyEditorCode('A', 'A', down)).toBe('unsaved');
  const bad = async () => ({ ok: false, json: async () => ({}) });
  expect(await classifyEditorCode('A', 'A', bad)).toBe('unsaved');
});
