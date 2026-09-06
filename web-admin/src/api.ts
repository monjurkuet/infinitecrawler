// Admin API client — Bearer token auth against the internal :8015 API.
const TOKEN_KEY = 'ic_admin_token'

export function getToken(): string | null {
  return sessionStorage.getItem(TOKEN_KEY)
}
export function setToken(t: string) { sessionStorage.setItem(TOKEN_KEY, t) }
export function clearToken() { sessionStorage.removeItem(TOKEN_KEY) }

async function request<T>(path: string, opts: RequestInit = {}): Promise<T> {
  const token = getToken()
  const res = await fetch(path, {
    ...opts,
    headers: {
      ...(opts.headers || {}),
      Authorization: `Bearer ${token ?? ''}`,
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

export interface Overview {
  total_listings: number
  total_search_results: number
  listings_today: number
  search_results_today: number
  emails_today: number
  listings_with_phone: number
  listings_with_email: number
  coverage_pct: number | null
}

export interface ThroughputPoint {
  day: string
  listings: number
}

export interface RecentActivity {
  listings_last_hour: number
  emails_last_hour: number
  linkedin_last_day: number
}

export interface SystemStatus {
  crawlers_running: number
  crawler_pids: number[]
  queues: Array<{ key: string; pending: number; processing: number; completed: number; failed: number }>
  database: Record<string, number>
}

export interface CrawlerProcess {
  unit: string
  active_state: string
  sub_state: string
  n_restarts: number | null
  memory_current: number | null
  main_pid: number | null
}

export interface QueueStats {
  key: string
  pending: number
  processing: number
  completed: number
  failed: number
}

export interface FailedItem {
  url: string
  attempts: number
  last_attempt: string | null
  error: string | null
}

export interface DaemonUnit {
  unit: string
  active_state: string
  sub_state: string
  description: string
  n_restarts: number | null
  memory_current: number | null
  main_pid: number | null
  last_state_change: string | null
}

export interface DaemonLogs {
  unit: string
  lines: number
  logs: string
  error?: string
}

export interface BbbStats {
  total: number
  by_state: Record<string, number>
  by_source: Record<string, number>
}

export const api = {
  overview: () => request<Overview>('/admin/overview'),
  throughput: () => request<{ series: ThroughputPoint[] }>('/admin/throughput'),
  recentActivity: () => request<RecentActivity>('/admin/recent-activity'),
  coverage: () => request<any>('/admin/coverage'),
  status: () => request<SystemStatus>('/admin/status'),
  crawlers: () => request<CrawlerProcess[]>('/admin/crawlers'),
  daemons: () => request<DaemonUnit[]>('/admin/daemons'),
  queue: () => request<QueueStats[]>('/admin/queue'),
  failed: (prefix: string) => request<FailedItem[]>(`/admin/queue/${prefix}/failed`),
  daemonLogs: (unit: string, tail = 200, filter?: string) => {
    const params = new URLSearchParams({ tail: String(tail) })
    if (filter) params.set('filter', filter)
    return request<DaemonLogs>(`/admin/daemons/${unit}/logs?${params}`)
  },
  bbbStats: () => request<BbbStats>('/api/bbb/stats'),
  bbbJobs: () => request<any[]>('/api/bbb/jobs'),
}
