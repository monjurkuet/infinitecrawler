const TOKEN_KEY = 'ic_token'

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY)
}

export function setToken(t: string) {
  localStorage.setItem(TOKEN_KEY, t)
}

export function clearToken() {
  localStorage.removeItem(TOKEN_KEY)
}

async function request<T>(path: string, opts: RequestInit = {}): Promise<T> {
  const token = getToken()
  const res = await fetch(path, {
    ...opts,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(opts.headers || {}),
    },
  })
  if (res.status === 401) {
    clearToken()
    window.location.href = '/login'
    throw new Error('unauthorized')
  }
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || `HTTP ${res.status}`)
  }
  return res.json()
}

export interface User {
  id: number
  email: string
  entitlement: { tier: string; rows_limit: number | null }
  searches_run: number
  rows_exported: number
}

export interface Lead {
  id: number
  name: string | null
  category: string | null
  rating: number | null
  review_count: number | null
  address: string | null
  phone: string | null
  website: string | null
  plus_code: string | null
  latitude: number | null
  longitude: number | null
  emails: string[]
  linkedin_url: string | null
  linkedin_title: string | null
  source_url: string | null
  is_claimed: boolean | null
  sector_id: string | null
  created_at: string | null
  updated_at: string | null
}

export interface LeadDetail extends Lead {
  payload: Record<string, unknown> | null
  social_links: Record<string, unknown> | null
  emails_full: Array<Record<string, unknown>>
  linkedin_profiles: Array<Record<string, unknown>>
}

export interface LeadListResponse {
  total: number
  page: number
  size: number
  items: Lead[]
}

export interface Stats {
  total_listings: number
  total_emails: number
  total_linkedin: number
  rows_exported: number
  searches_run: number
  rows_limit: number | null
}

export const api = {
  register: (email: string, password: string) =>
    request<{ token: string; user: User }>('/auth/register', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    }),
  login: (email: string, password: string) =>
    request<{ token: string; user: User }>('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ email, password }),
    }),
  me: () => request<User>('/auth/me'),
  leads: (params: URLSearchParams) =>
    request<LeadListResponse>(`/premium/leads?${params.toString()}`),
  leadDetail: (id: number) => request<LeadDetail>(`/premium/leads/${id}`),
  stats: () => request<Stats>('/premium/stats'),
  exportCsvUrl: (params: URLSearchParams) => `/premium/export.csv?${params.toString()}`,
}
