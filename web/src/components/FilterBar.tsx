export interface Filters {
  city: string
  category: string
  minRating: string
  hasEmail: boolean
  q: string
}

interface Props {
  values: Filters
  onChange: (f: Filters) => void
  onSearch: () => void
}

export default function FilterBar({ values, onChange, onSearch }: Props) {
  const set = (k: keyof Filters, v: string | boolean) => onChange({ ...values, [k]: v })

  return (
    <form
      className="grid grid-cols-2 gap-3 md:grid-cols-5"
      onSubmit={e => {
        e.preventDefault()
        onSearch()
      }}
    >
      <input
        placeholder="City (e.g. Dhaka)"
        value={values.city}
        onChange={e => set('city', e.target.value)}
        className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 text-sm outline-none focus:border-indigo-500"
      />
      <input
        placeholder="Category"
        value={values.category}
        onChange={e => set('category', e.target.value)}
        className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 text-sm outline-none focus:border-indigo-500"
      />
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
      <label className="flex items-center gap-2 text-sm text-slate-300">
        <input
          type="checkbox"
          checked={values.hasEmail}
          onChange={e => set('hasEmail', e.target.checked)}
          className="h-4 w-4"
        />
        has email
      </label>
      <input
        placeholder="Search name/address…"
        value={values.q}
        onChange={e => set('q', e.target.value)}
        className="rounded border border-slate-700 bg-slate-800 px-3 py-1.5 text-sm outline-none focus:border-indigo-500"
      />
      <button
        type="submit"
        className="col-span-2 rounded bg-indigo-600 py-1.5 text-sm font-medium hover:bg-indigo-500 md:col-span-5"
      >
        Search
      </button>
    </form>
  )
}
