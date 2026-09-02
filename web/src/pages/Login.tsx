import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { useAuth } from '../auth'

export default function Login() {
  const { login } = useAuth()
  const nav = useNavigate()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [err, setErr] = useState('')
  const [busy, setBusy] = useState(false)

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault()
    setErr('')
    setBusy(true)
    try {
      await login(email, password)
      nav('/')
    } catch (ex) {
      setErr(ex instanceof Error ? ex.message : 'Login failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 grid place-items-center px-4">
      <form onSubmit={onSubmit} className="w-full max-w-sm rounded-xl border border-slate-800 bg-slate-900 p-8 shadow-xl">
        <h1 className="text-2xl font-semibold">InfiniteCrawler Leads</h1>
        <p className="mt-1 text-sm text-slate-400">Sign in to your premium account</p>
        {err && <div className="mt-4 rounded bg-red-500/10 border border-red-500/40 px-3 py-2 text-sm text-red-300">{err}</div>}
        <label className="mt-5 block text-sm">
          Email
          <input type="email" required value={email} onChange={e => setEmail(e.target.value)} autoFocus
            className="mt-1 w-full rounded border border-slate-700 bg-slate-800 px-3 py-2 text-sm outline-none focus:border-indigo-500" />
        </label>
        <label className="mt-4 block text-sm">
          Password
          <input type="password" required value={password} onChange={e => setPassword(e.target.value)}
            className="mt-1 w-full rounded border border-slate-700 bg-slate-800 px-3 py-2 text-sm outline-none focus:border-indigo-500" />
        </label>
        <button type="submit" disabled={busy}
          className="mt-6 w-full rounded bg-indigo-600 py-2 text-sm font-medium hover:bg-indigo-500 disabled:opacity-50">
          {busy ? 'Signing in…' : 'Sign in'}
        </button>
        <p className="mt-4 text-center text-sm text-slate-400">
          No account? <Link to="/register" className="text-indigo-400 hover:underline">Sign up — free pro access</Link>
        </p>
      </form>
    </div>
  )
}
