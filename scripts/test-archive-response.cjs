const fs=require('node:fs');const vm=require('node:vm');const ts=require('typescript');const assert=require('node:assert/strict');
const source=fs.readFileSync('app/components/ArchiveCoverage.tsx','utf8');
const js=ts.transpileModule(source,{compilerOptions:{jsx:ts.JsxEmit.ReactJSX,module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
async function check(data,expectedError){
 const states=[];let index=0;let effect;const exports={};
 const context=vm.createContext({exports,require:name=>name==='react'?{useState:initial=>{const i=index++;states[i]=initial;return [initial,value=>states[i]=value]},useEffect:fn=>effect=fn}:{jsx:()=>null,jsxs:()=>null},AbortController,URLSearchParams,fetch:async()=>({ok:true,json:async()=>data}),setTimeout:fn=>{context.run=fn;return 1},clearTimeout(){}});
 vm.runInContext(js,context);exports.default({revision:0,onSelect(){}});effect();await context.run();
 assert.equal(states[5],false);assert.equal(typeof states[4],'number');
 if(expectedError){assert.match(states[6],/Redeploy the Railway worker/);assert.equal(states[3].length,0)}else{assert.equal(states[6],'');assert.equal(states[4],data.total)}
}
(async()=>{await check({symbols:[]},true);await check({symbols:null,total:0},true);await check({symbols:[],total:NaN},true);await check({symbols:[],total:0},false);await check({symbols:[],total:8000},false);console.log('Archive response regression passed')})().catch(e=>{console.error(e);process.exitCode=1});
