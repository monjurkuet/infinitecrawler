import { useEffect, useMemo, useState } from 'react'
import { useAuth } from '../auth'
import { api, getToken, type BbbLeadListResponse, type LeadListResponse, type Stats } from '../api'
import FilterBar, { type Filters } from '../components/FilterBar'
import LeadDrawer from '../components/LeadDrawer'

const EMPTY: Filters = { country: 'Bangladesh', city: '', category: '', minRating: '', hasEmail: false, q: '' }
const SIZE = 50

function toParams(f: Filters, page: number): URLSearchParams {
  const p = new URLSearchParams()
  if (f.country) p.set('country', f.country)
  if (f.city) p.set('city', f.city)
  if (f.category) p.set('category', f.category)
  if (f.minRating) p.set('min_rating', f.minRating)
  if (f.hasEmail) p.set('has_email', 'true')
  if (f.q) p.set('q', f.q)
  p.set('page', String(page))
  p.set('size', String(SIZE))
  return p
}

export default function Dashboard() {
  const { user, logout } = useAuth()
  const [tab, setTab] = useState<'gmaps' | 'bbb'>('gmaps')
  const [filters, setFilters] = useState<Filters>(EMPTY)
  const [applied, setApplied] = useState<Filters>(EMPTY)
  const [page, setPage] = useState(1)
  const [data, setData] = useState<LeadListResponse | null>(null)
  const [stats, setStats] = useState<Stats | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')
  const [drawerId, setDrawerId] = useState<number | null>(null)

  useEffect(() => {
    api.stats().then(setStats).catch(() => {})
  }, [data])

  useEffect(() => {
    setLoading(true)
    setErr('')
    api.leads(toParams(applied, page))
      .then(setData)
      .catch(e => setErr(String(e)))
      .finally(() => setLoading(false))
  }, [applied, page])

  function search() {
    setPage(1)
    setApplied({ ...filters })
  }

  async function exportCsv() {
    const p = toParams(applied, 1)
    p.delete('page')
    p.delete('size')
    const res = await fetch(`/premium/export.csv?${p.toString()}`, {
      headers: { Authorization: `Bearer ${getToken()}` },
    })
    if (!res.ok) {
      setErr(`Export failed: HTTP ${res.status}`)
      return
    }
    const blob = await res.blob()
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'infinitecrawler-leads.csv'
    a.click()
    URL.revokeObjectURL(url)
    api.stats().then(setStats).catch(() => {})
  }

  const totalPages = useMemo(() => (data ? Math.max(1, Math.ceil(data.total / data.size)) : 1), [data])

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <header className="sticky top-0 z-30 border-b border-slate-800 bg-slate-950/80 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3">
          <div>
            <span className="text-lg font-semibold">InfiniteCrawler</span>
            <span className="ml-2 rounded bg-indigo-600/20 px-2 py-0.5 text-xs text-indigo-300">
              {user?.entitlement.tier}
            </span>
          </div>
          <div className="flex items-center gap-4 text-sm text-slate-400">
            {stats && (
              <>
                <span>{stats.total_listings.toLocaleString()} listings</span>
                <span>{stats.total_emails.toLocaleString()} emails</span>
                <span>{stats.rows_exported.toLocaleString()} exported</span>
              </>
            )}
            <span className="text-slate-500">{user?.email}</span>
            <button onClick={logout} className="rounded border border-slate-700 px-2 py-1 hover:bg-slate-800">
              Logout
            </button>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl p-4">
        <div className="mb-4 flex gap-2">
          <button
            onClick={() => setTab('gmaps')}
            className={`rounded px-3 py-1.5 text-sm font-medium ${tab === 'gmaps' ? 'bg-indigo-600' : 'border border-slate-700 hover:bg-slate-800'}`}
          >
            Google Maps leads
          </button>
          <button
            onClick={() => setTab('bbb')}
            className={`rounded px-3 py-1.5 text-sm font-medium ${tab === 'bbb' ? 'bg-indigo-600' : 'border border-slate-700 hover:bg-slate-800'}`}
          >
            BBB leads (US trades)
          </button>
        </div>

        {tab === 'gmaps' ? (
        <>
        <div className="rounded-xl border border-slate-800 bg-slate-900 p-4">
          <FilterBar values={filters} onChange={setFilters} onSearch={search} />
          <div className="mt-3 flex items-center justify-between">
            <div className="text-sm text-slate-400">
              {loading ? 'Loading…' : data ? `${data.total.toLocaleString()} results` : 'Run a search'}
            </div>
            <button
              onClick={exportCsv}
              disabled={!data || data.total === 0}
              className="rounded bg-emerald-600 px-3 py-1.5 text-sm font-medium hover:bg-emerald-500 disabled:opacity-40"
            >
              Export CSV ({data?.total.toLocaleString() ?? 0} rows)
            </button>
          </div>
          {err && <div className="mt-3 rounded bg-red-500/10 border border-red-500/40 px-3 py-2 text-sm text-red-300">{err}</div>}
        </div>

        <div className="mt-4 overflow-x-auto rounded-xl border border-slate-800">
          <table className="w-full text-sm">
            <thead className="bg-slate-900 text-left text-slate-400">
              <tr>
                <th className="px-3 py-2">Name</th>
                <th className="px-3 py-2">Category</th>
                <th className="px-3 py-2">Rating</th>
                <th className="px-3 py-2">Phone</th>
                <th className="px-3 py-2">Emails</th>
                <th className="px-3 py-2">LinkedIn</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {data?.items.map(l => (
                <tr
                  key={l.id}
                  onClick={() => setDrawerId(l.id)}
                  className="cursor-pointer bg-slate-950 hover:bg-slate-900"
                >
                  <td className="max-w-xs truncate px-3 py-2">{l.name ?? <span className="text-slate-600">—</span>}</td>
                  <td className="max-w-[10rem] truncate px-3 py-2 text-slate-400">{l.category ?? '—'}</td>
                  <td className="px-3 py-2">
                    {l.rating != null ? (
                      <span className="text-amber-300">★ {l.rating}</span>
                    ) : (
                      <span className="text-slate-600">—</span>
                    )}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs">{l.phone ?? <span className="text-slate-600">—</span>}</td>
                  <td className="px-3 py-2">
                    {l.emails.length > 0 ? (
                      <span className="rounded bg-emerald-600/20 px-2 py-0.5 text-xs text-emerald-300">
                        {l.emails.length}
                      </span>
                    ) : (
                      <span className="text-slate-600">—</span>
                    )}
                  </td>
                  <td className="px-3 py-2">
                    {l.linkedin_url ? (
                      <span className="text-indigo-400">✓</span>
                    ) : (
                      <span className="text-slate-600">—</span>
                    )}
                  </td>
                </tr>
              ))}
              {!loading && data?.items.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-3 py-8 text-center text-slate-500">
                    No results match these filters.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        {data && totalPages > 1 && (
          <div className="mt-4 flex items-center justify-center gap-2 text-sm">
            <button
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={page === 1}
              className="rounded border border-slate-700 px-3 py-1 disabled:opacity-40"
            >
              ← Prev
            </button>
            <span className="text-slate-400">
              Page {page} of {totalPages.toLocaleString()}
            </span>
            <button
              onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
              className="rounded border border-slate-700 px-3 py-1 disabled:opacity-40"
            >
              Next →
            </button>
          </div>
        )}

      {drawerId != null && <LeadDrawer id={drawerId} onClose={() => setDrawerId(null)} />}
        </>
        ) : (
          <BbbTab />
        )}
      </main>
    </div>
  )
}

function BbbTab() {
  const [state, setState] = useState('')
  const [q, setQ] = useState('')
  const [accredited, setAccredited] = useState('')
  const [hasWebsite, setHasWebsite] = useState('')
  const [applied, setApplied] = useState({ state: '', q: '', accredited: '', hasWebsite: '' })
  const [page, setPage] = useState(1)
  const [data, setData] = useState<BbbLeadListResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState('')

  function params(pg: number): URLSearchParams {
    const p = new URLSearchParams()
    if (applied.state) p.set('state', applied.state)
    if (applied.q) p.set('q', applied.q)
    if (applied.accredited) p.set('accredited', applied.accredited)
    if (applied.hasWebsite) p.set('has_website', applied.hasWebsite)
    p.set('page', String(pg))
    p.set('size', '50')
    return p
  }

  useEffect(() => {
    setLoading(true)
    setErr('')
    api.bbbLeads(params(page))
      .then(setData)
      .catch(e => setErr(String(e)))
      .finally(() => setLoading(false))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [applied, page])

  function search() {
    setPage(1)
    setApplied({ state, q, accredited, hasWebsite })
  }

  async function exportCsv() {
    const p = params(1)
    p.delete('page')
    p.delete('size')
    const res = await fetch(`/premium/bbb-leads/export.csv?${p.toString()}`, {
      headers: { Authorization: `Bearer ${getToken()}` },
    })
    if (!res.ok) {
      setErr(`Export failed: HTTP ${res.status}`)
      return
    }
    const blob = await res.blob()
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'ic-bbb-leads.csv'
    a.click()
    URL.revokeObjectURL(url)
  }

  const totalPages = Math.max(1, Math.ceil((data?.total ?? 0) / (data?.size ?? 50)))
  const inputCls = 'rounded border border-slate-700 bg-slate-950 px-2 py-1.5 text-sm'

  return (
    <>
      <div className="rounded-xl border border-slate-800 bg-slate-900 p-4">
        <div className="flex flex-wrap gap-2">
          <input value={state} onChange={e => setState(e.target.value.toUpperCase())} placeholder="State (e.g. NE)" maxLength={2} className={`${inputCls} w-32`} />
          <input value={q} onChange={e => setQ(e.target.value)} placeholder="Name, city, trade…" className={`${inputCls} w-56`} />
          <select value={accredited} onChange={e => setAccredited(e.target.value)} className={inputCls}>
            <option value="">Accredited: any</option>
            <option value="true">Accredited only</option>
            <option value="false">Not accredited</option>
          </select>
          <select value={hasWebsite} onChange={e => setHasWebsite(e.target.value)} className={inputCls}>
            <option value="">Website: any</option>
            <option value="true">Has website</option>
            <option value="false">No website</option>
          </select>
          <button onClick={search} className="rounded bg-indigo-600 px-3 py-1.5 text-sm font-medium hover:bg-indigo-500">Search</button>
        </div>
        <div className="mt-3 flex items-center justify-between">
          <div className="text-sm text-slate-400">
            {loading ? 'Loading…' : data ? `${data.total.toLocaleString()} results` : 'Run a search'}
          </div>
          <button
            onClick={exportCsv}
            disabled={!data || data.total === 0}
            className="rounded bg-emerald-600 px-3 py-1.5 text-sm font-medium hover:bg-emerald-500 disabled:opacity-40"
          >
            Export CSV ({data?.total.toLocaleString() ?? 0} rows)
          </button>
        </div>
        {err && <div className="mt-3 rounded border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-300">{err}</div>}
      </div>

      <div className="mt-4 overflow-x-auto rounded-xl border border-slate-800">
        <table className="w-full text-sm">
          <thead className="bg-slate-900 text-left text-slate-400">
            <tr>
              <th className="px-3 py-2">Business</th>
              <th className="px-3 py-2">Location</th>
              <th className="px-3 py-2">Rating</th>
              <th className="px-3 py-2">Phone</th>
              <th className="px-3 py-2">Website</th>
              <th className="px-3 py-2">Email</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {data?.items.map(b => (
              <tr key={b.id} className="bg-slate-950 hover:bg-slate-900">
                <td className="max-w-xs truncate px-3 py-2">
                  {b.business_name ?? '—'}
                  {b.accredited && <span className="ml-2 rounded bg-indigo-600/20 px-1.5 py-0.5 text-xs text-indigo-300">BBB</span>}
                </td>
                <td className="px-3 py-2 text-slate-400">{[b.city, b.state].filter(Boolean).join(', ') || '—'}</td>
                <td className="px-3 py-2 text-amber-300">{b.rating ?? '—'}</td>
                <td className="px-3 py-2 font-mono text-xs">{b.phone ?? '—'}</td>
                <td className="max-w-[12rem] truncate px-3 py-2">
                  {b.website ? <a href={b.website} target="_blank" rel="noreferrer" className="text-emerald-400 hover:underline">site ↗</a> : <span className="text-slate-600">—</span>}
                </td>
                <td className="px-3 py-2">{b.email ? <span className="text-emerald-300">{b.email}</span> : <span className="text-slate-600">—</span>}</td>
              </tr>
            ))}
            {!loading && data?.items.length === 0 && (
              <tr>
                <td colSpan={6} className="px-3 py-8 text-center text-slate-500">No results match these filters.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {data && totalPages > 1 && (
        <div className="mt-4 flex items-center justify-center gap-2 text-sm">
          <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} className="rounded border border-slate-700 px-3 py-1 disabled:opacity-40">← Prev</button>
          <span className="text-slate-400">Page {page} of {totalPages.toLocaleString()}</span>
          <button onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page === totalPages} className="rounded border border-slate-700 px-3 py-1 disabled:opacity-40">Next →</button>
        </div>
      )}
    </>
  )
}
