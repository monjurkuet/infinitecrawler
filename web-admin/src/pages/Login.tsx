import { useState } from 'react'
import { useAdmin } from '../auth'

export default function Login() {
  const { setToken } = useAdmin()
  const [t, setT] = useState('')
  const [err, setErr] = useState('')

  function submit(e: React.FormEvent) {
    e.preventDefault()
    if (!t) return setErr('Enter your admin token')
    setToken(t)
    window.location.href = '/'
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 grid place-items-center px-4">
      <form onSubmit={submit} className="w-full max-w-md rounded-xl border border-slate-800 bg-slate-900 p-8 shadow-xl">
        <h1 className="text-2xl font-semibold">InfiniteCrawler Admin</h1>
        <p className="mt-1 text-sm text-slate-400">Enter your admin token</p>
        {err && <div className="mt-4 rounded bg-red-500/10 border border-red-500/40 px-3 py-2 text-sm text-red-300">{err}</div>}
        <input
          type="password"
          value={t}
          onChange={e => setT(e.target.value)}
          placeholder="Bearer token"
          className="mt-4 w-full rounded border border-slate-700 bg-slate-800 px-3 py-2 text-sm outline-none focus:border-indigo-500"
        />
        <button type="submit" className="mt-4 w-full rounded bg-indigo-600 py-2 text-sm font-medium hover:bg-indigo-500">
          Sign in
        </button>
      </form>
    </div>
  )
}
