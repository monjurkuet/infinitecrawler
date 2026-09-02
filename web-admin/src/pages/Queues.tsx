import { useEffect, useState } from 'react'
import { api } from '../api'
import { useAdmin } from '../auth'

export default function Queues() {
  const { token } = useAdmin()
  const [queues, setQueues] = useState<any[]>([])

  const refresh = () => {
    if (!token) return
    api.queue().then(setQueues)
  }

  useEffect(() => { refresh(); const t = setInterval(refresh, 15000); return () => clearInterval(t) }, [token])

  const totalPending = queues.reduce((s, q) => s + q.pending, 0)
  const totalFailed = queues.reduce((s, q) => s + q.failed, 0)

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div className="rounded-xl bg-red-900/20 border border-red-900 p-4">
        <div className="text-xs uppercase text-red-300">Total failed</div>
        <div className="text-2xl font-semibold">{totalFailed.toLocaleString()}</div>
      </div>
      <div className="rounded-xl bg-amber-900/20 border border-amber-900 p-4">
        <div className="text-xs uppercase text-amber-300">Total pending</div>
        <div className="text-2xl font-semibold">{totalPending.toLocaleString()}</div>
      </div>
      <div className="rounded-xl bg-emerald-900/20 border border-emerald-900 p-4">
        <div className="text-xs uppercase text-emerald-300">Total completed</div>
        <div className="text-2xl font-semibold">
          {queues.reduce((s, q) => s + q.completed, 0).toLocaleString()}
        </div>
      </div>
      <table className="lg:col-span-3 rounded-xl border border-slate-800 bg-slate-900 w-full">
        <thead className="bg-slate-950 text-slate-400">
          <tr>
            <th className="px-4 py-3 text-left text-xs">Key</th>
            <th className="px-4 py-3 text-right text-xs">Pending</th>
            <th className="px-4 py-3 text-right text-xs">Processing</th>
            <th className="px-4 py-3 text-right text-xs">Failed</th>
            <th className="px-4 py-3 text-right text-xs">Completed</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {queues.map(q => (
            <tr key={q.key} className="hover:bg-slate-800/50">
              <td className="px-4 py-2 font-mono text-xs">{q.key}</td>
              <td className="px-4 py-2 text-right font-mono">{q.pending.toLocaleString()}</td>
              <td className="px-4 py-2 text-right context">{q.processing.toLocaleString()}</td>
              <td className="px-4 py-2 text-right text-red-300">{q.failed.toLocaleString()}</td>
              <td className="px-4 py-2 text-right text-emerald-300">{q.completed.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="lg:col-span-3 text-xs text-slate-500 px-1">
        Auto-refresh every 15s
      </div>
    </div>
  )
}
