import { Message } from '../hooks/useGenie'
import QueryResults from './QueryResults'

const styles = {
  container: {
    marginBottom: '16px',
    display: 'flex',
    flexDirection: 'column' as const,
  },
  userContainer: {
    alignItems: 'flex-end',
  },
  assistantContainer: {
    alignItems: 'flex-start',
  },
  bubble: {
    maxWidth: '80%',
    padding: '12px 16px',
    borderRadius: '12px',
    fontSize: '15px',
    lineHeight: 1.5,
  },
  userBubble: {
    background: '#1e3a5f',
    color: 'white',
    borderBottomRightRadius: '4px',
  },
  assistantBubble: {
    background: '#f0f4f8',
    color: '#1a1a1a',
    borderBottomLeftRadius: '4px',
  },
  pendingBubble: {
    background: '#fff3cd',
    color: '#856404',
  },
  failedBubble: {
    background: '#f8d7da',
    color: '#721c24',
  },
  timestamp: {
    fontSize: '11px',
    color: '#888',
    marginTop: '4px',
  },
  query: {
    marginTop: '12px',
    padding: '12px',
    background: '#1e1e1e',
    color: '#d4d4d4',
    borderRadius: '8px',
    fontSize: '13px',
    fontFamily: 'Monaco, Consolas, monospace',
    overflowX: 'auto' as const,
    whiteSpace: 'pre-wrap' as const,
  },
  queryLabel: {
    fontSize: '11px',
    color: '#888',
    marginBottom: '4px',
    fontWeight: 600,
  },
}

interface Props {
  message: Message
}

export default function MessageBubble({ message }: Props) {
  const isUser = message.role === 'user'

  const bubbleStyle = {
    ...styles.bubble,
    ...(isUser ? styles.userBubble : styles.assistantBubble),
    ...(message.status === 'pending' && !isUser ? styles.pendingBubble : {}),
    ...(message.status === 'failed' && !isUser ? styles.failedBubble : {}),
  }

  return (
    <div
      style={{
        ...styles.container,
        ...(isUser ? styles.userContainer : styles.assistantContainer),
      }}
    >
      <div style={bubbleStyle}>
        {message.content}
      </div>

      {message.query && (
        <div style={{ maxWidth: '80%', marginTop: '8px' }}>
          <div style={styles.queryLabel}>Generated SQL:</div>
          <div style={styles.query}>{message.query}</div>
        </div>
      )}

      {message.results && message.results.length > 0 && (
        <div style={{ maxWidth: '100%', marginTop: '12px' }}>
          <QueryResults results={message.results} />
        </div>
      )}

      <div style={styles.timestamp}>
        {message.timestamp.toLocaleTimeString()}
      </div>
    </div>
  )
}
