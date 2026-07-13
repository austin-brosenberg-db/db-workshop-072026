import { useState, useEffect } from 'react'

const styles = {
  container: {
    height: '100%',
    display: 'flex',
    flexDirection: 'column' as const,
    background: 'white',
    borderRadius: '12px',
    boxShadow: '0 4px 12px rgba(0,0,0,0.1)',
    overflow: 'hidden',
  },
  iframe: {
    flex: 1,
    width: '100%',
    border: 'none',
  },
  loading: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    height: '100%',
    color: '#666',
    fontSize: '16px',
  },
  fallback: {
    display: 'flex',
    flexDirection: 'column' as const,
    alignItems: 'center',
    justifyContent: 'center',
    height: '100%',
    color: '#666',
    textAlign: 'center' as const,
    padding: '40px',
  },
  fallbackIcon: {
    fontSize: '64px',
    marginBottom: '20px',
  },
  fallbackTitle: {
    fontSize: '20px',
    fontWeight: 600,
    marginBottom: '12px',
    color: '#333',
  },
  fallbackText: {
    fontSize: '14px',
    lineHeight: 1.6,
    maxWidth: '450px',
    marginBottom: '24px',
  },
  link: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: '8px',
    color: 'white',
    background: '#1e3a5f',
    textDecoration: 'none',
    padding: '14px 28px',
    borderRadius: '8px',
    fontWeight: 600,
    fontSize: '15px',
    transition: 'background 0.2s',
  },
  note: {
    marginTop: '20px',
    fontSize: '12px',
    color: '#888',
    maxWidth: '400px',
  },
}

// Dashboard configuration
const DASHBOARD_URL = 'https://fevm-illumia-demo.cloud.databricks.com/dashboardsv3/01f17d4496921f258f21500e4029ce2c/published?o=7474656906295934'

export default function DashboardEmbed() {
  const [loading, setLoading] = useState(true)
  const [canEmbed, setCanEmbed] = useState(false)

  useEffect(() => {
    // Check if we're on a databricks.com domain (where embedding is allowed)
    const hostname = window.location.hostname
    const isDatabricksDomain = hostname.endsWith('.databricks.com') ||
                                hostname.endsWith('.azuredatabricks.net')
    setCanEmbed(isDatabricksDomain)

    if (!isDatabricksDomain) {
      setLoading(false)
    }
  }, [])

  const handleLoad = () => {
    setLoading(false)
  }

  // Show fallback with link when not on Databricks domain
  if (!canEmbed) {
    return (
      <div style={styles.container}>
        <div style={styles.fallback}>
          <div style={styles.fallbackIcon}>📊</div>
          <div style={styles.fallbackTitle}>Illumia Campus Analytics Dashboard</div>
          <div style={styles.fallbackText}>
            The dashboard opens in a new tab due to security restrictions.
            When this app is deployed to Databricks, the dashboard will be embedded directly.
          </div>
          <a
            href={DASHBOARD_URL}
            target="_blank"
            rel="noopener noreferrer"
            style={styles.link}
            onMouseOver={e => (e.currentTarget.style.background = '#2d5a87')}
            onMouseOut={e => (e.currentTarget.style.background = '#1e3a5f')}
          >
            Open Dashboard ↗
          </a>
          <div style={styles.note}>
            Dashboard includes: Revenue by Location, Food Waste Analysis,
            Cardholder Engagement, and Spending by Housing Area
          </div>
        </div>
      </div>
    )
  }

  return (
    <div style={styles.container}>
      {loading && (
        <div style={styles.loading}>Loading dashboard...</div>
      )}
      <iframe
        src={DASHBOARD_URL}
        style={{
          ...styles.iframe,
          display: loading ? 'none' : 'block',
        }}
        onLoad={handleLoad}
        title="Illumia Campus Analytics Dashboard"
        allow="fullscreen"
      />
    </div>
  )
}
