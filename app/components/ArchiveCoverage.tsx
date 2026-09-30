'use client';
import { useEffect, useState } from 'react';

type Row = {instrument:string; symbol:string; interval:string; from_date:string; to_date:string; candles:number};
const periods = ['1m','5m','15m','30m','1h','1d','1w','1mo'];
const pageSize = 50;
export default function ArchiveCoverage({revision,onSelect}:{revision:number;onSelect:(instrument:string)=>void}) {
  const [search,setSearch]=useState(''); const [interval,setInterval]=useState(''); const [page,setPage]=useState(1);
  const [rows,setRows]=useState<Row[]>([]); const [total,setTotal]=useState(0); const [loading,setLoading]=useState(true); const [error,setError]=useState('');
  useEffect(()=>{
    const abort=new AbortController();setLoading(true);setError('');
    const timer=setTimeout(async()=>{
      try {
        const query=new URLSearchParams({search,limit:String(pageSize),offset:String((page-1)*pageSize)});
        if(interval)query.set('interval',interval);
        const response=await fetch(`/api/symbols?${query}`,{cache:'no-store',signal:abort.signal});
        const data=await response.json();if(!response.ok)throw new Error(data.detail||'Could not load archive');
        if(abort.signal.aborted)return;
        if(!Array.isArray(data.symbols)||!Number.isSafeInteger(data.total)||data.total<0)throw new Error('Archive pagination is unavailable. Redeploy the Railway worker to the latest version, then refresh this page.');
        const lastPage=Math.max(1,Math.ceil(data.total/pageSize));
        if(page>lastPage){setPage(lastPage);return;}
        setRows(data.symbols);setTotal(data.total);
      } catch(e){if(!abort.signal.aborted){setError((e as Error).message);setRows([]);}}
      finally{if(!abort.signal.aborted)setLoading(false);}
    },250);
    return()=>{clearTimeout(timer);abort.abort();};
  },[search,interval,page,revision]);
  return <>
    <div className="archivefilters"><label>SEARCH STOCK<input value={search} onChange={e=>{setSearch(e.target.value);setPage(1);}} placeholder="Ticker or instrument key"/></label>
      <label>TIMEFRAME<select value={interval} onChange={e=>{setInterval(e.target.value);setPage(1);}}><option value="">All timeframes</option>{periods.map(p=><option key={p}>{p}</option>)}</select></label></div>
    {loading&&<p className="footnote" role="status">Loading archive page…</p>}
    {error&&<p className="alert danger" role="alert">{error}</p>}
    <div className="tablewrap"><table><thead><tr><th>INSTRUMENT</th><th>PERIOD</th><th>RANGE</th><th className="numeric">CANDLES</th></tr></thead><tbody>{rows.map(r=><tr key={`${r.instrument}:${r.interval}`}><td><button className="plain symbolbutton" onClick={()=>onSelect(r.instrument)}>{r.symbol}</button><small className="instrumentkey">{r.instrument}</small></td><td><span className="period">{r.interval}</span></td><td>{r.from_date} → {r.to_date}</td><td className="numeric">{r.candles.toLocaleString()}</td></tr>)}</tbody></table></div>
    {!loading&&!error&&!rows.length&&<div className="empty"><h3>No matching archive data</h3><p>Try another search, or start an import.</p></div>}
    <div className="pagination"><span>{total.toLocaleString()} stock/timeframe entries · page {page} / {Math.max(1,Math.ceil(total/pageSize))} · up to {pageSize} rows</span><div><button className="plain" disabled={loading||page===1} onClick={()=>setPage(page-1)}>Previous</button><button className="plain" disabled={loading||page*pageSize>=total} onClick={()=>setPage(page+1)}>Next</button></div></div>
  </>;
}
