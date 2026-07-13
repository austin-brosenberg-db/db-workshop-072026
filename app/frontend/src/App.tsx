import { useState } from 'react'
import ChatInterface from './components/ChatInterface'

const styles = {
  container: {
    minHeight: '100vh',
    display: 'flex',
    flexDirection: 'column' as const,
  },
  header: {
    background: 'linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%)',
    color: 'white',
    padding: '16px 24px',
    boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
  },
  title: {
    fontSize: '24px',
    fontWeight: 600,
    margin: 0,
  },
  subtitle: {
    fontSize: '14px',
    opacity: 0.9,
    marginTop: '4px',
  },
  main: {
    flex: 1,
    display: 'flex',
    flexDirection: 'column' as const,
    maxWidth: '1000px',
    width: '100%',
    margin: '0 auto',
    padding: '24px',
  },
}

function App() {
  return (
    <div style={styles.container}>
      <header style={styles.header}>
        <h1 style={styles.title}>Illumia Campus Analytics</h1>
        <p style={styles.subtitle}>
          Ask questions about revenue, dining operations, cardholder engagement, and more
        </p>
      </header>
      <main style={styles.main}>
        <ChatInterface />
      </main>
    </div>
  )
}

export default App
