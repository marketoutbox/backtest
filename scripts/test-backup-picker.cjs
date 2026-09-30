// Regression: backup preparation no longer invokes any native file picker.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const ts = require('typescript');
const source = fs.readFileSync('app/components/DataBackup.tsx', 'utf8');
assert.equal(/showDirectoryPicker|showSaveFilePicker|createWritable/.test(source), false);
const ast = ts.createSourceFile('DataBackup.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
let handler;
function visit(node) {
  if (ts.isFunctionDeclaration(node) && node.name?.text === 'prepare') handler = node.getText(ast);
  ts.forEachChild(node, visit);
}
visit(ast);
assert.ok(handler);
const javascript = ts.transpileModule(handler, {compilerOptions: {target: ts.ScriptTarget.ES2022}}).outputText;
(async () => {
  let complete;
  const calls = []; const state = {};
  const context = vm.createContext({pending:{current:false},
    setBusy:value => state.busy=value, setError:value => state.error=value, setMessage:value => state.message=value,
    request:(path,method) => {calls.push({path,method}); return new Promise(resolve => {complete=resolve;});},
    refresh:async () => {state.refreshed=true;},
  });
  vm.runInContext(javascript, context);
  const first = context.prepare();
  await context.prepare();
  assert.equal(calls.length,1,'double clicks enqueue only one request');
  assert.deepEqual(calls[0],{path:'',method:'POST'});
  assert.equal(state.busy,true);
  complete({id:'server-job',status:'queued'}); await first;
  assert.equal(state.busy,false);
  assert.equal(state.refreshed,true);
  assert.match(state.message,/close this page/);
  context.request=async () => {throw new Error('Worker unavailable');};
  await context.prepare();
  assert.equal(state.busy,false); assert.equal(context.pending.current,false);
  assert.equal(state.error,'Worker unavailable');
  console.log('PASS: no file picker, single background request, status refresh and request failure recovery');
})().catch(error => {console.error(error); process.exitCode=1;});
