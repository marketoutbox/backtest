// Exercise the actual download handler with a delayed native picker.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const ts = require('typescript');
const source = fs.readFileSync('app/components/DataBackup.tsx', 'utf8');
const ast = ts.createSourceFile('DataBackup.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
let handler;
function visit(node) {
  if (ts.isFunctionDeclaration(node) && node.name?.text === 'download') handler = node.getText(ast);
  ts.forEachChild(node, visit);
}
visit(ast);
assert.ok(handler, 'download handler exists');
const javascript = ts.transpileModule(handler, {compilerOptions: {target: ts.ScriptTarget.ES2022}}).outputText;
function harness() {
  let resolvePicker, rejectPicker;
  const state = {busy: false, choosing: false, calls: 0, fetches: 0};
  const context = vm.createContext({AbortController, console,
    controller: {current: null}, folderPickerOpen: false,
    setBusy: value => state.busy = value,
    setChoosingFolder: value => state.choosing = value,
    setError: value => state.error = value,
    setMessage: value => state.message = value,
    window: {showDirectoryPicker() {
      state.calls++;
      return new Promise((resolve, reject) => {resolvePicker = resolve; rejectPicker = reject;});
    }},
    fetch: async () => {state.fetches++; return {ok: false, status: 409, text: async () => '{"detail":"Active jobs"}'};},
  });
  vm.runInContext(javascript, context);
  return {context, state, resolve: value => resolvePicker(value), reject: value => rejectPicker(value)};
}
(async () => {
  const first = harness();
  const pending = first.context.download();
  assert.equal(first.state.busy, true);
  assert.equal(first.state.choosing, true);
  assert.match(first.state.message, /Choose a folder/);
  await first.context.download();
  await first.context.download();
  assert.equal(first.state.calls, 1, 'rapid clicks must not open a second picker');
  first.reject(Object.assign(new Error('cancelled'), {name: 'AbortError'}));
  await pending;
  assert.equal(first.state.busy, false);
  assert.equal(first.state.choosing, false);
  assert.equal(first.context.controller.current, null);
  assert.equal(first.context.folderPickerOpen, false);
  const retry = first.context.download();
  assert.equal(first.state.calls, 2, 'cancelled selection can be retried');
  first.resolve({}); await retry;
  assert.equal(first.state.fetches, 1);
  assert.match(first.state.error, /Active jobs/);
  assert.equal(first.state.busy, false);

  const unmounted = harness();
  const old = unmounted.context.download();
  unmounted.context.controller.current.abort();
  unmounted.resolve({}); await old;
  assert.equal(unmounted.state.fetches, 0, 'unmounted picker must not start a backup');
  assert.equal(unmounted.context.folderPickerOpen, false);

  const existing = harness();
  existing.context.folderPickerOpen = true;
  await existing.context.download();
  assert.equal(existing.state.calls, 0);
  assert.match(existing.state.error, /already open/);

  const browserFailure = harness();
  const failed = browserFailure.context.download();
  browserFailure.reject(Object.assign(new Error('File picker already active'), {name: 'InvalidStateError'}));
  await failed;
  assert.equal(browserFailure.state.busy, false);
  assert.match(browserFailure.state.error, /Close it/);
  console.log('PASS: repeated clicks, cancelled selection/retry, unmount, existing picker and browser errors');
})().catch(error => {console.error(error); process.exitCode = 1;});
