import { useEffect, useState } from 'react'
import { api, type FilterOptions } from '../api'

export interface Filters {
  country: string
  city: string
  category: string
  minRating: string
  hasEmail: boolean
  q: string
}

const EMPTY_OPTS: FilterOptions = {
  categories: [],
  countries: ['Bangladesh', 'India', 'Canada', 'United Kingdom', 'United States'],
  cities_by_country: {
    Bangladesh: ['Dhaka', 'Chattogram', 'Sylhet', 'Rajshahi', 'Khulna', 'Barisal', 'Rangpur', 'Mymensingh', 'Gazipur', 'Narayanganj'],
  },
}

interface Props {
  values: Filters
  onChange: (f: Filters) => void
  onSearch: () => void
}

export default function FilterBar({ values, onChange, onSearch }: Props) {
  const [opts, setOpts] = useState<FilterOptions>(EMPTY_OPTS)

  useEffect(() => {
    api.filters()
      .then(setOpts)
      .catch(() => {})  // keep hardcoded fallback
  }, [])

  const set = (k: keyof Filters, v: string | boolean) => {
    const next = { ...values, [k]: v }
    // When country changes and it's NOT Bangladesh, cities fall back to free-text
    // because we don't have canonical city lists for other countries.
    if (k === 'country') next.city = ''
    onChange(next)
  }

  const cities = opts.cities_by_country[values.country] ?? []
  const showCityDropdown = values.country === 'Bangladesh' && cities.length > 0

  return (
    <form
      className="grid grid-cols-2 gap-3 md:grid-cols-6"
      onSubmit={e => {
        e.preventDefault()
        onSearch()
      }}
    >
      <select
        value={values.country}
        onChange={e => set('country', e.target.value)}
        className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 text-sm outline-none focus:border-indigo-500"
      >
        <option value="">All countries</option>
        {opts.countries.map(c => (
          <option key={c} value={c}>{c}</option>
        ))}
      </select>

      {showCityDropdown ? (
        <select
          value={values.city}
          onChange={e => set('city', e.target.value)}
          className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 text-sm outline-none focus:border-indigo-500"
        >
          <option value="">All cities (BD)</option>
          {cities.map(c => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
      ) : (
        <input
          placeholder={values.country ? `City in ${values.country}` : 'City'}
          value={values.city}
          onChange={e => set('city', e.target.value)}
          className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 text-sm outline-none focus:border-indigo-500"
        />
      )}

      <select
        value={values.category}
        onChange={e => set('category', e.target.value)}
        className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 text-sm outline-none focus:border-indigo-500"
      >
        <option value="">All categories</option>
        {opts.categories.map(c => (
          <option key={c} value={c}>{c}</option>
        ))}
      </select>

      <select
        value={values.minRating}
        onChange={e => set('minRating', e.target.value)}
        className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 text-sm outline-none focus:border-indigo-500"
      >
        <option value="">Any rating</option>
        <option value="3">3+ stars</option>
        <option value="3.5">3.5+ stars</option>
        <option value="4">4+ stars</option>
        <option value="4.5">4.5+ stars</option>
      </select>

      <input
        placeholder="Search name/address…"
        value={values.q}
        onChange={e => set('q', e.target.value)}
        className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 text-sm outline-none focus:border-indigo-500"
      />

      <label className="flex items-center gap-2 text-sm text-slate-300">
        <input
          type="checkbox"
          checked={values.hasEmail}
          onChange={e => set('hasEmail', e.target.checked)}
          className="h-4 w-4"
        />
        has email
      </label>

      <button
        type="submit"
        className="col-span-2 rounded bg-indigo-600 py-1.5 text-sm font-medium hover:bg-indigo-500 md:col-span-6"
      >
        Search
      </button>
    </form>
  )
}
