import { useState } from 'react'
import ChatInterface from './components/ChatInterface'
import DashboardEmbed from './components/DashboardEmbed'

type Page = 'genie' | 'dashboard'

const styles = {
  container: {
    display: 'flex',
    minHeight: '100vh',
  },
  sidebar: {
    width: '220px',
    background: 'linear-gradient(180deg, #1e3a5f 0%, #162d4a 100%)',
    color: 'white',
    display: 'flex',
    flexDirection: 'column' as const,
    flexShrink: 0,
  },
  logo: {
    padding: '20px',
    borderBottom: '1px solid rgba(255,255,255,0.1)',
  },
  logoTitle: {
    fontSize: '18px',
    fontWeight: 600,
    margin: 0,
  },
  logoSubtitle: {
    fontSize: '12px',
    opacity: 0.7,
    marginTop: '4px',
  },
  nav: {
    padding: '16px 0',
    flex: 1,
  },
  navItem: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
    padding: '12px 20px',
    cursor: 'pointer',
    transition: 'background 0.2s',
    borderLeft: '3px solid transparent',
  },
  navItemActive: {
    background: 'rgba(255,255,255,0.1)',
    borderLeftColor: '#4dabf7',
  },
  navIcon: {
    fontSize: '20px',
    width: '24px',
    textAlign: 'center' as const,
  },
  navLabel: {
    fontSize: '14px',
    fontWeight: 500,
  },
  main: {
    flex: 1,
    display: 'flex',
    flexDirection: 'column' as const,
    background: '#f5f5f5',
    overflow: 'hidden',
  },
  header: {
    background: 'white',
    padding: '16px 24px',
    borderBottom: '1px solid #e0e0e0',
    boxShadow: '0 1px 3px rgba(0,0,0,0.05)',
  },
  headerTitle: {
    fontSize: '20px',
    fontWeight: 600,
    margin: 0,
    color: '#1a1a1a',
  },
  content: {
    flex: 1,
    padding: '24px',
    overflow: 'hidden',
    display: 'flex',
    flexDirection: 'column' as const,
  },
}

const pages: { id: Page; label: string; icon: string }[] = [
  { id: 'genie', label: 'Ask Genie', icon: '✨' },
  { id: 'dashboard', label: 'Dashboard', icon: '📊' },
]

function App() {
  const [currentPage, setCurrentPage] = useState<Page>('genie')

  const getPageTitle = () => {
    switch (currentPage) {
      case 'genie':
        return 'Ask Genie'
      case 'dashboard':
        return 'Campus Analytics Dashboard'
      default:
        return ''
    }
  }

  return (
    <div style={styles.container}>
      {/* Sidebar */}
      <aside style={styles.sidebar}>
        <div style={styles.logo}>
          <h1 style={styles.logoTitle}>Illumia</h1>
          <p style={styles.logoSubtitle}>Campus Analytics</p>
        </div>
        <nav style={styles.nav}>
          {pages.map(page => (
            <div
              key={page.id}
              style={{
                ...styles.navItem,
                ...(currentPage === page.id ? styles.navItemActive : {}),
              }}
              onClick={() => setCurrentPage(page.id)}
              onMouseOver={e => {
                if (currentPage !== page.id) {
                  e.currentTarget.style.background = 'rgba(255,255,255,0.05)'
                }
              }}
              onMouseOut={e => {
                if (currentPage !== page.id) {
                  e.currentTarget.style.background = 'transparent'
                }
              }}
            >
              <span style={styles.navIcon}>{page.icon}</span>
              <span style={styles.navLabel}>{page.label}</span>
            </div>
          ))}
        </nav>
      </aside>

      {/* Main Content */}
      <main style={styles.main}>
        <header style={styles.header}>
          <h2 style={styles.headerTitle}>{getPageTitle()}</h2>
        </header>
        <div style={styles.content}>
          {currentPage === 'genie' && <ChatInterface />}
          {currentPage === 'dashboard' && <DashboardEmbed />}
        </div>
      </main>
    </div>
  )
}

export default App
