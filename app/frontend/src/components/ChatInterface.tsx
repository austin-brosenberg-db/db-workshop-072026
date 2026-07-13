import { useState, useRef, useEffect } from 'react'
import { useGenie } from '../hooks/useGenie'
import MessageBubble from './MessageBubble'

const styles = {
  container: {
    display: 'flex',
    flexDirection: 'column' as const,
    height: 'calc(100vh - 140px)',
    background: 'white',
    borderRadius: '12px',
    boxShadow: '0 4px 12px rgba(0,0,0,0.1)',
    overflow: 'hidden',
  },
  messagesContainer: {
    flex: 1,
    overflowY: 'auto' as const,
    padding: '20px',
  },
  emptyState: {
    display: 'flex',
    flexDirection: 'column' as const,
    alignItems: 'center',
    justifyContent: 'center',
    height: '100%',
    color: '#666',
    textAlign: 'center' as const,
  },
  emptyIcon: {
    fontSize: '48px',
    marginBottom: '16px',
  },
  emptyTitle: {
    fontSize: '18px',
    fontWeight: 600,
    marginBottom: '8px',
  },
  emptyText: {
    fontSize: '14px',
    maxWidth: '400px',
    lineHeight: 1.5,
  },
  suggestions: {
    display: 'flex',
    flexWrap: 'wrap' as const,
    gap: '8px',
    marginTop: '20px',
    justifyContent: 'center',
  },
  suggestion: {
    padding: '8px 16px',
    background: '#f0f4f8',
    border: '1px solid #d0d7de',
    borderRadius: '20px',
    cursor: 'pointer',
    fontSize: '13px',
    transition: 'all 0.2s',
  },
  inputContainer: {
    display: 'flex',
    gap: '12px',
    padding: '16px 20px',
    borderTop: '1px solid #e0e0e0',
    background: '#fafafa',
  },
  input: {
    flex: 1,
    padding: '12px 16px',
    fontSize: '15px',
    border: '1px solid #d0d7de',
    borderRadius: '8px',
    outline: 'none',
  },
  button: {
    padding: '12px 24px',
    background: '#1e3a5f',
    color: 'white',
    border: 'none',
    borderRadius: '8px',
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: '15px',
    transition: 'background 0.2s',
  },
  buttonDisabled: {
    background: '#a0a0a0',
    cursor: 'not-allowed',
  },
  clearButton: {
    padding: '12px 16px',
    background: 'transparent',
    color: '#666',
    border: '1px solid #d0d7de',
    borderRadius: '8px',
    cursor: 'pointer',
    fontSize: '14px',
  },
}

const SUGGESTIONS = [
  'What are the top 5 merchants by revenue?',
  'Show me food waste by dining hall',
  'Which housing areas spend the most?',
  'How many cardholders are in each engagement tier?',
]

export default function ChatInterface() {
  const { messages, loading, sendMessage, clearConversation } = useGenie()
  const [input, setInput] = useState('')
  const messagesEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (input.trim() && !loading) {
      sendMessage(input.trim())
      setInput('')
    }
  }

  const handleSuggestion = (suggestion: string) => {
    if (!loading) {
      sendMessage(suggestion)
    }
  }

  return (
    <div style={styles.container}>
      <div style={styles.messagesContainer}>
        {messages.length === 0 ? (
          <div style={styles.emptyState}>
            <div style={styles.emptyIcon}>🎓</div>
            <div style={styles.emptyTitle}>Illumia Campus Analytics</div>
            <div style={styles.emptyText}>
              Ask questions about campus operations, dining, commerce, and student engagement.
              Powered by Databricks Genie.
            </div>
            <div style={styles.suggestions}>
              {SUGGESTIONS.map((s, i) => (
                <button
                  key={i}
                  style={styles.suggestion}
                  onClick={() => handleSuggestion(s)}
                  onMouseOver={e => (e.currentTarget.style.background = '#e0e7ed')}
                  onMouseOut={e => (e.currentTarget.style.background = '#f0f4f8')}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <>
            {messages.map(message => (
              <MessageBubble key={message.id} message={message} />
            ))}
            <div ref={messagesEndRef} />
          </>
        )}
      </div>

      <form style={styles.inputContainer} onSubmit={handleSubmit}>
        {messages.length > 0 && (
          <button
            type="button"
            style={styles.clearButton}
            onClick={clearConversation}
          >
            Clear
          </button>
        )}
        <input
          type="text"
          value={input}
          onChange={e => setInput(e.target.value)}
          placeholder="Ask a question about campus analytics..."
          style={styles.input}
          disabled={loading}
        />
        <button
          type="submit"
          style={{
            ...styles.button,
            ...(loading || !input.trim() ? styles.buttonDisabled : {}),
          }}
          disabled={loading || !input.trim()}
        >
          {loading ? 'Thinking...' : 'Send'}
        </button>
      </form>
    </div>
  )
}
