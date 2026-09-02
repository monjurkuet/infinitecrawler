import { createContext, useContext, useEffect, useState, type ReactNode } from 'react'
import { api, getToken, setToken, clearToken, type User } from './api'

interface AuthCtx {
  user: User | null
  ready: boolean
  login: (email: string, password: string) => Promise<void>
  register: (email: string, password: string) => Promise<void>
  logout: () => void
}

const Ctx = createContext<AuthCtx | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [ready, setReady] = useState(false)

  useEffect(() => {
    if (!getToken()) {
      setReady(true)
      return
    }
    api.me()
      .then(setUser)
      .catch(() => clearToken())
      .finally(() => setReady(true))
  }, [])

  async function login(email: string, password: string) {
    const r = await api.login(email, password)
    setToken(r.token)
    setUser(r.user)
  }

  async function register(email: string, password: string) {
    const r = await api.register(email, password)
    setToken(r.token)
    setUser(r.user)
  }

  function logout() {
    clearToken()
    setUser(null)
    window.location.href = '/login'
  }

  return <Ctx.Provider value={{ user, ready, login, register, logout }}>{children}</Ctx.Provider>
}

export function useAuth() {
  const ctx = useContext(Ctx)
  if (!ctx) throw new Error('useAuth outside provider')
  return ctx
}
