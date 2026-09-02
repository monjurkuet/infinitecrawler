import { useEffect, useState } from 'react'
import { api } from '../api'
import { useAdmin } from '../auth'

export default function Overview() {
  const { token } = useAdmin()
  const [status, setStatus] = useState<any>(null)
  const [throughput, setThroughput] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!token) return
    Promise.all([
      api.status(),
      api.throughput(),
    ]).then(([s, th]) => {
      setStatus(s)
      setThroughput(th.series ?? [])
      setLoading(false)
    })
  }, [token])

  if (loading || !status) return <div className="text-slate-500 py-20 text-center">Loading…</div>

  const cards = [
    { label: 'Total listings', value: status.database?.total_listings ?? '—' },
    { label: 'Search results', value: status.database?.total_search_results ?? '—' },
    { label: 'Running crawlers', value: status.crawlers_running },
    { label: 'Pending queue', value: status.queues?.reduce((a: number, q: any) => a + (q.pending ?? 0), 0) ?? 0 },
  ]

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {cards.map(c => (
          <div key={c.label} className="rounded-xl border border-slate-800 bg-slate-900 p-4">
            <div className="text-xs uppercase tracking-wide text-slate-500">{c.label}</div>
            <div className="mt-1 text-2xl font-semibold">{c.value.toLocaleString()}</div>
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-4">
          <h2 className="font-semibold mb-4">24h throughput</h2>
          <div className="h-56 flex items-end gap-1">
            {throughput.slice(-24).map((pt: any, i: number) => (
              <div key={i} className="flex-1 flex flex-col items-center">
                <div className="w-full bg-indigo-600/60 rounded-t" style={{ height: `${Math.min(100, (pt.listings ?? 0) / Math.max(...throughput.map(p => p.listings ?? 0)) * 100)}%` }} />
                <div className="text-[9px] text-slate-500 mt-1 rotate-45 origin-left whitespace-nowrap">{pt.day?.slice(5, 10)}</div>
              </div>
            ))}
          </div>
        </div>
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-4 text-sm">
          <h2 className="font-semibold mb-4">Queues</h2>
          <table className="w-full text-left">
            <thead className="text-slate-500 text-xs uppercase">
              <tr>
                <th>Key</th><th className="text-right">Pending</th><th className="text-right">Processing</th><th className="text-right">Completed</th><th className="text-right">Failed</th>
              </tr>
            </thead>
            <tbody>
              {status.queues?.map((q: any) => (
                <tr key={q.key} className="border-t border-slate-800">
                  <td className="py-2 font-mono text-xs">{q.key}</td>
                  <td className="py-2 text-right">{q.pending.toLocaleString()}</td>
                  <td className="py-2 text-right">{q.processing}</td>
                  <td className="py-2 text-right text-emerald-300">{q.completed.toLocaleString()}</td>
                  <td className="py-2 text-right text-red-300">{q.failed}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
