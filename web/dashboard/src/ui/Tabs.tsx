type TabItem = {
  id: string
  label: string
}

type TabsProps = {
  items: TabItem[]
  activeID: string
  onChange: (id: string) => void
}

export function Tabs({ items, activeID, onChange }: TabsProps): JSX.Element {
  return (
    <div className="tabs" role="tablist">
      {items.map((item) => (
        <button
          aria-selected={item.id === activeID}
          className={item.id === activeID ? 'active' : undefined}
          key={item.id}
          onClick={() => onChange(item.id)}
          role="tab"
          type="button"
        >
          {item.label}
        </button>
      ))}
    </div>
  )
}
