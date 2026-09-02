import { useEffect, useState } from 'react'
import { api, type DaemonLogs } from '../api'
import { useAdmin } from '../auth'

const UNITS = [
  'search-daemon', 'listing-daemon', 'places-api-daemon',
  'nearby-scanner-daemon', 'db_email_extract.py', 'db_linkedin_firehose.py',
  'db_linkedin_profile_backfill.py', 'db_linkedin_company_enrich.py',
  'db_classify.py', 'phantom_sweeper.py', 'api.main', 'api_premium.main'
] as const

export default function Logs() {
  const { token } = useAdmin()
  const [unit, setUnit] = useState<string>('listing-daemon')
  const [tail, setTail] = useState(200)
  const [filter, setFilter] = useState('')
  const [logs, setLogs] = useState<DaemonLogs | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!token) return
    api.daemonLogs(unit, tail, filter || undefined)
      .then(setLogs)
      .finally(() => setLoading(false))
  }, [token, unit, tail, filter])

  const lines = logs?.logs?.split('\n') ?? []

  return (
    <div className="space-y-3">
      <div className="flex gap-2 flex-wrap items-center text-sm">
        <select value={unit} onChange={e => setUnit(e.target.value)}
          className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 outline-none focus:border-indigo-500">
          {UNITS.map(u => <option key={u} value={u}>{u}</option>)}
        </select>
        <input type="number" value={tail} min={10} max={1000}
          onChange={e => setTail(Math.min(1000, Math.max(10, +e.target.value)))}
          className="w-20 rounded border border-slate-700 bg-slate-800 px-2 py-1.5 text-center outline-none focus:border-indigo-500" />
        <span className="text-slate-500">tail</span>
        <input placeholder="filter…" value={filter} onChange={e => setFilter(e.target.value)}
          className="flex-1 rounded border border-slate-700 bg-slate-800 px-3 py-1.5 outline-none focus:border-indigo-500" />
        <button onClick={() => setTimeout(() => api.daemonLogs(unit, tail, filter).then(setLogs), 0)}
          className="rounded bg-indigo-600 px-3 py-1.5 text-sm hover:bg-indigo-500">
          Reload
        </button>
      </div>
      <div className="rounded-xl border border-slate-800 bg-slate-950 p-4 font-mono text-xs overflow-x-auto max-h-[70vh] overflow-y-auto">
        {loading && <div className="text-slate-500">Loading {tail} lines…</div>}
        {lines.length === 0 && !loading && <div className="text-slate-600">No logs yet.</div>}
        {lines.map((ln, i) => (
          <div key={i} className="whitespace-pre-wrap">{ln}</div>
        ))}
      </div>
    </div>
  )
}
