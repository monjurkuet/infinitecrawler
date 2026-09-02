import { useEffect, useState } from 'react'
import { api, type DaemonUnit } from '../api'
import { useAdmin } from '../auth'

export default function Daemons() {
  const { token } = useAdmin()
  const [units, setUnits] = useState<DaemonUnit[]>([])

  const refresh = () => {
    if (!token) return
    api.daemons().then(setUnits)
  }

  useEffect(() => { refresh(); const t = setInterval(refresh, 30000); return () => clearInterval(t) }, [token])

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900 overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-slate-950 text-slate-400">
          <tr>
            <th className="px-4 py-3 text-left">Unit</th>
            <th className="px-4 py-3 text-left">Status</th>
            <th className="px-4 py-3 text-left">Restarts</th>
            <th className="px-4 py-3 text-left">Memory</th>
            <th className="px-4 py-3 text-left">Last activity</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {units.map(d => (
            <tr key={d.unit} className="hover:bg-slate-800/50">
              <td className="px-4 py-3 font-mono text-xs">{d.unit}</td>
              <td className="px-4 py-3">
                <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs ${
                  d.active_state === 'active' ? 'bg-emerald-900/40 text-emerald-300' :
                  d.active_state === 'activating' ? 'bg-amber-900/40 text-amber-300' :
                  'bg-red-900/40 text-red-300'
                }`}>
                  {d.active_state}
                </span>
              </td>
              <td className="px-4 py-3">{d.n_restarts ?? 0}</td>
              <td className="px-4 py-3 font-mono text-xs">{d.memory_current ? `${(d.memory_current / 1048576).toFixed(1)}M` : '—'}</td>
              <td className="px-4 py-3 text-slate-500 text-xs">{d.last_state_change ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="p-3 text-xs text-slate-500 border-t border-slate-800">
        Auto-refresh every 30s
      </div>
    </div>
  )
}
