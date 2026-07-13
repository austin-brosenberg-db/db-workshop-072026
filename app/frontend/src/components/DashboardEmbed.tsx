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
  error: {
    display: 'flex',
    flexDirection: 'column' as const,
    alignItems: 'center',
    justifyContent: 'center',
    height: '100%',
    color: '#d32f2f',
    textAlign: 'center' as const,
    padding: '40px',
  },
  errorIcon: {
    fontSize: '48px',
    marginBottom: '16px',
  },
  errorText: {
    fontSize: '14px',
    maxWidth: '400px',
  },
}

interface AppConfig {
  dashboard_url: string | null
  dashboard_id: string | null
  genie_space_id: string | null
}

export default function DashboardEmbed() {
  const [loading, setLoading] = useState(true)
  const [iframeLoading, setIframeLoading] = useState(true)
  const [canEmbed, setCanEmbed] = useState(false)
  const [dashboardUrl, setDashboardUrl] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    // Check if we're on a Databricks domain (where embedding is allowed)
    const hostname = window.location.hostname
    const isDatabricksDomain = hostname.endsWith('.databricks.com') ||
                                hostname.endsWith('.azuredatabricks.net') ||
                                hostname.endsWith('.databricksapps.com')
    setCanEmbed(isDatabricksDomain)

    // Fetch dashboard URL from backend config
    fetch('/api/config')
      .then(res => {
        if (!res.ok) throw new Error('Failed to fetch config')
        return res.json()
      })
      .then((config: AppConfig) => {
        if (config.dashboard_url) {
          setDashboardUrl(config.dashboard_url)
        } else {
          setError('Dashboard not configured. Please set DASHBOARD_ID environment variable.')
        }
        setLoading(false)
      })
      .catch(err => {
        setError(`Failed to load configuration: ${err.message}`)
        setLoading(false)
      })
  }, [])

  const handleIframeLoad = () => {
    setIframeLoading(false)
  }

  // Loading state while fetching config
  if (loading) {
    return (
      <div style={styles.container}>
        <div style={styles.loading}>Loading dashboard configuration...</div>
      </div>
    )
  }

  // Error state
  if (error) {
    return (
      <div style={styles.container}>
        <div style={styles.error}>
          <div style={styles.errorIcon}>!</div>
          <div style={styles.fallbackTitle}>Configuration Error</div>
          <div style={styles.errorText}>{error}</div>
        </div>
      </div>
    )
  }

  // No dashboard URL configured
  if (!dashboardUrl) {
    return (
      <div style={styles.container}>
        <div style={styles.fallback}>
          <div style={styles.fallbackIcon}>!</div>
          <div style={styles.fallbackTitle}>Dashboard Not Configured</div>
          <div style={styles.fallbackText}>
            The DASHBOARD_ID environment variable is not set.
            Please configure it in app.yaml and redeploy.
          </div>
        </div>
      </div>
    )
  }

  // Show fallback with link when not on Databricks domain
  if (!canEmbed) {
    return (
      <div style={styles.container}>
        <div style={styles.fallback}>
          <div style={styles.fallbackIcon}>!</div>
          <div style={styles.fallbackTitle}>Illumia Campus Analytics Dashboard</div>
          <div style={styles.fallbackText}>
            The dashboard opens in a new tab due to security restrictions.
            When this app is deployed to Databricks, the dashboard will be embedded directly.
          </div>
          <a
            href={dashboardUrl}
            target="_blank"
            rel="noopener noreferrer"
            style={styles.link}
            onMouseOver={e => (e.currentTarget.style.background = '#2d5a87')}
            onMouseOut={e => (e.currentTarget.style.background = '#1e3a5f')}
          >
            Open Dashboard
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
      {iframeLoading && (
        <div style={styles.loading}>Loading dashboard...</div>
      )}
      <iframe
        src={dashboardUrl}
        style={{
          ...styles.iframe,
          display: iframeLoading ? 'none' : 'block',
        }}
        onLoad={handleIframeLoad}
        title="Illumia Campus Analytics Dashboard"
        allow="fullscreen"
      />
    </div>
  )
}
