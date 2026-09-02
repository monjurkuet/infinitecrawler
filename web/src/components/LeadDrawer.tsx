import { useEffect, useState } from 'react'
import { api, type LeadDetail } from '../api'

interface Props {
  id: number
  onClose: () => void
}

export default function LeadDrawer({ id, onClose }: Props) {
  const [lead, setLead] = useState<LeadDetail | null>(null)
  const [err, setErr] = useState('')

  useEffect(() => {
    setLead(null)
    setErr('')
    api.leadDetail(id).then(setLead).catch(e => setErr(String(e)))
  }, [id])

  return (
    <div className="fixed inset-0 z-40 flex justify-end bg-black/50" onClick={onClose}>
      <div
        className="h-full w-full max-w-xl overflow-y-auto bg-slate-900 p-6 shadow-2xl"
        onClick={e => e.stopPropagation()}
      >
        <div className="mb-4 flex items-start justify-between">
          <h2 className="text-lg font-semibold">{lead?.name ?? 'Loading…'}</h2>
          <button onClick={onClose} className="rounded px-2 py-1 text-slate-400 hover:bg-slate-800">
            ✕
          </button>
        </div>
        {err && <div className="text-red-400 text-sm">{err}</div>}
        {lead && (
          <dl className="space-y-3 text-sm">
            <Field label="Category" value={lead.category} />
            <Field label="Rating" value={lead.rating != null ? `${lead.rating} (${lead.review_count ?? 0} reviews)` : null} />
            <Field label="Address" value={lead.address} />
            <Field label="Phone" value={lead.phone} mono />
            <Field label="Website" value={lead.website} link />
            <Field label="Plus code" value={lead.plus_code} mono />
            <Field
              label="Coordinates"
              value={lead.latitude != null ? `${lead.latitude}, ${lead.longitude}` : null}
              mono
            />
            <Field label="Sector" value={lead.sector_id} />
            <Section title={`Emails (${lead.emails.length})`}>
              {lead.emails.length === 0 && <p className="text-slate-500">none</p>}
              {lead.emails.map(em => (
                <div key={em} className="font-mono text-emerald-300">{em}</div>
              ))}
            </Section>
            <Section title={`LinkedIn (${lead.linkedin_profiles?.length ?? 0})`}>{
              (lead.linkedin_profiles && lead.linkedin_profiles.length > 0) ? (
                <div className="space-y-2">
                  {lead.linkedin_profiles.slice(0, 3).map(p => (
                    <div key={String(p.profile_url)} className="rounded bg-slate-950 p-2">
                      <a href={String(p.profile_url)} target="_blank" rel="noreferrer" className="text-indigo-400 hover:underline">
                        {String(p.full_name || p.profile_title || p.profile_url)}
                      </a>
                      <div className="text-xs text-slate-400 mt-0.5 space-x-2">
                        {!!p.profile_title && <span>{String(p.profile_title)}</span>}
                        {!!p.company_name && <span>@ {String(p.company_name)}</span>}
                        {!!p.profile_location && <span>· {String(p.profile_location)}</span>}
                        {!!p.profile_country && <span>{String(p.profile_country)}</span>}
                        {!!p.connections_count && <span>· {String(p.connections_count)} connections</span>}
                      </div>
                    </div>
                  ))}
                  {(lead.linkedin_profiles ?? []).length > 3 && (
                    <p className="text-xs text-slate-500">+{(lead.linkedin_profiles?.length ?? 0) - 3} more</p>
                  )}
                </div>
              ) : (
                <p className="text-slate-500">no profiles linked</p>
              )
            }</Section>
            <Section title="Google Maps">
              {lead.source_url && (
                <a href={lead.source_url} target="_blank" rel="noreferrer" className="text-indigo-400 break-all hover:underline">
                  {lead.source_url}
                </a>
              )}
            </Section>
          </dl>
        )}
      </div>
    </div>
  )
}

function Field({ label, value, mono, link }: { label: string; value?: string | null; mono?: boolean; link?: boolean }) {
  return (
    <div className="grid grid-cols-3 gap-2">
      <dt className="text-slate-500">{label}</dt>
      <dd className={`col-span-2 ${mono ? 'font-mono' : ''}`}>
        {value ? (link && value.startsWith('http') ? (
          <a href={value} target="_blank" rel="noreferrer" className="text-indigo-400 hover:underline">{value}</a>
        ) : value) : <span className="text-slate-600">—</span>}
      </dd>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded border border-slate-800 p-3">
      <div className="mb-1 text-xs font-medium uppercase tracking-wide text-slate-500">{title}</div>
      {children}
    </div>
  )
}
