import { BrowserRouter, Routes, Route, Navigate, NavLink } from 'react-router-dom'
import { AdminAuthProvider, useAdmin } from './auth'
import Login from './pages/Login'
import Overview from './pages/Overview'
import Daemons from './pages/Daemons'
import Queues from './pages/Queues'
import Logs from './pages/Logs'

function Shell({ children }: { children: React.ReactNode }) {
  const { logout } = useAdmin()
  const link = ({ isActive }: { isActive: boolean }) =>
    `text-sm px-3 py-1.5 rounded ${isActive ? 'bg-slate-800 text-slate-100' : 'text-slate-400 hover:text-slate-200'}`
  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <header className="sticky top-0 z-40 border-b border-slate-800 bg-slate-950/80 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3">
          <div className="flex items-center gap-6">
            <span className="text-lg font-semibold">Admin</span>
            <nav className="flex gap-2">
              <NavLink to="/" end className={link}>Overview</NavLink>
              <NavLink to="/daemons" className={link}>Daemons</NavLink>
              <NavLink to="/queues" className={link}>Queues</NavLink>
              <NavLink to="/logs" className={link}>Logs</NavLink>
            </nav>
          </div>
          <button onClick={logout} className="rounded border border-slate-700 px-3 py-1 text-sm hover:bg-slate-800">
            Sign out
          </button>
        </div>
      </header>
      <main className="mx-auto max-w-7xl p-4">{children}</main>
    </div>
  )
}

function Protected({ children }: { children: React.ReactNode }) {
  const { token } = useAdmin()
  if (!token) return <Navigate to="/login" replace />
  return <Shell>{children}</Shell>
}

export default function App() {
  return (
    <AdminAuthProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/" element={<Protected><Overview /></Protected>} />
          <Route path="/daemons" element={<Protected><Daemons /></Protected>} />
          <Route path="/queues" element={<Protected><Queues /></Protected>} />
          <Route path="/logs" element={<Protected><Logs /></Protected>} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </AdminAuthProvider>
  )
}