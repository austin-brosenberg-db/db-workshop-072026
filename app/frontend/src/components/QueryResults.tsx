const styles = {
  container: {
    overflowX: 'auto' as const,
    border: '1px solid #e0e0e0',
    borderRadius: '8px',
    background: 'white',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse' as const,
    fontSize: '13px',
  },
  th: {
    background: '#f5f5f5',
    padding: '10px 12px',
    textAlign: 'left' as const,
    fontWeight: 600,
    borderBottom: '2px solid #e0e0e0',
    whiteSpace: 'nowrap' as const,
  },
  td: {
    padding: '8px 12px',
    borderBottom: '1px solid #f0f0f0',
  },
  trEven: {
    background: '#fafafa',
  },
  empty: {
    padding: '20px',
    textAlign: 'center' as const,
    color: '#666',
  },
  footer: {
    padding: '8px 12px',
    fontSize: '12px',
    color: '#666',
    background: '#f9f9f9',
    borderTop: '1px solid #e0e0e0',
  },
}

interface Props {
  results: Record<string, unknown>[]
}

export default function QueryResults({ results }: Props) {
  if (!results || results.length === 0) {
    return (
      <div style={styles.container}>
        <div style={styles.empty}>No results</div>
      </div>
    )
  }

  const columns = Object.keys(results[0])
  const displayResults = results.slice(0, 100) // Limit display to 100 rows

  return (
    <div style={styles.container}>
      <table style={styles.table}>
        <thead>
          <tr>
            {columns.map(col => (
              <th key={col} style={styles.th}>
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayResults.map((row, i) => (
            <tr key={i} style={i % 2 === 0 ? {} : styles.trEven}>
              {columns.map(col => (
                <td key={col} style={styles.td}>
                  {formatValue(row[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {results.length > 100 && (
        <div style={styles.footer}>
          Showing 100 of {results.length} rows
        </div>
      )}
    </div>
  )
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '-'
  }
  if (typeof value === 'number') {
    // Format numbers nicely
    if (Number.isInteger(value)) {
      return value.toLocaleString()
    }
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
  }
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No'
  }
  return String(value)
}
