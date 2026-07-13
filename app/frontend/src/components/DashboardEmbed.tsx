import { useState } from 'react'

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
  error: {
    display: 'flex',
    flexDirection: 'column' as const,
    alignItems: 'center',
    justifyContent: 'center',
    height: '100%',
    color: '#666',
    textAlign: 'center' as const,
    padding: '40px',
  },
  errorIcon: {
    fontSize: '48px',
    marginBottom: '16px',
  },
  errorTitle: {
    fontSize: '18px',
    fontWeight: 600,
    marginBottom: '8px',
    color: '#333',
  },
  errorText: {
    fontSize: '14px',
    lineHeight: 1.5,
    maxWidth: '400px',
  },
  link: {
    color: '#1e3a5f',
    textDecoration: 'none',
    marginTop: '16px',
    padding: '10px 20px',
    background: '#f0f4f8',
    borderRadius: '8px',
    fontWeight: 500,
  },
}

// Dashboard configuration
const DASHBOARD_URL = 'https://fevm-illumia-demo.cloud.databricks.com/dashboardsv3/01f17d4496921f258f21500e4029ce2c/published?o=7474656906295934'

export default function DashboardEmbed() {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)
  const dashboardUrl = DASHBOARD_URL

  const handleLoad = () => {
    setLoading(false)
  }

  const handleError = () => {
    setLoading(false)
    setError(true)
  }

  if (error) {
    return (
      <div style={styles.container}>
        <div style={styles.error}>
          <div style={styles.errorIcon}>📊</div>
          <div style={styles.errorTitle}>Dashboard Unavailable</div>
          <div style={styles.errorText}>
            The embedded dashboard could not be loaded. This may be due to authentication
            or the dashboard not being published.
          </div>
          <a
            href={dashboardUrl.replace('/embed/', '/')}
            target="_blank"
            rel="noopener noreferrer"
            style={styles.link}
          >
            Open Dashboard in New Tab
          </a>
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
        src={dashboardUrl}
        style={{
          ...styles.iframe,
          display: loading ? 'none' : 'block',
        }}
        onLoad={handleLoad}
        onError={handleError}
        title="Illumia Campus Analytics Dashboard"
        allow="fullscreen"
      />
    </div>
  )
}
