import { createContext, useContext, useState, type ReactNode } from 'react'
import { getToken, setToken, clearToken } from './api'

interface AdminCtx {
  token: string | null
  setToken: (t: string) => void
  logout: () => void
}

const Ctx = createContext<AdminCtx | null>(null)

export function AdminAuthProvider({ children }: { children: ReactNode }) {
  const [token, setTok] = useState<string | null>(getToken())

  return (
    <Ctx.Provider value={{
      token,
      setToken: t => { setToken(t); setTok(t) },
      logout: () => { clearToken(); setTok(null); window.location.href = '/login' },
    }}>
      {children}
    </Ctx.Provider>
  )
}

export function useAdmin() {
  const ctx = useContext(Ctx)
  if (!ctx) throw new Error('useAdmin outside provider')
  return ctx
}
