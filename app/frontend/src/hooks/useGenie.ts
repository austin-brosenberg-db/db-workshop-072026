import { useState, useCallback } from 'react'

export interface Message {
  id: string
  role: 'user' | 'assistant'
  content: string
  query?: string
  results?: Record<string, unknown>[]
  status: 'pending' | 'completed' | 'failed'
  timestamp: Date
}

interface ChatResponse {
  conversation_id: string
  message_id: string
  status: string
  content?: string
  query?: string
  results?: Record<string, unknown>[]
  error?: string
}

export function useGenie() {
  const [messages, setMessages] = useState<Message[]>([])
  const [conversationId, setConversationId] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const sendMessage = useCallback(async (question: string) => {
    setLoading(true)
    setError(null)

    // Add user message immediately
    const userMessage: Message = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: question,
      status: 'completed',
      timestamp: new Date(),
    }
    setMessages(prev => [...prev, userMessage])

    // Add pending assistant message
    const assistantId = `assistant-${Date.now()}`
    const pendingMessage: Message = {
      id: assistantId,
      role: 'assistant',
      content: 'Thinking...',
      status: 'pending',
      timestamp: new Date(),
    }
    setMessages(prev => [...prev, pendingMessage])

    try {
      // Start chat request
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question,
          conversation_id: conversationId,
        }),
      })

      if (!response.ok) {
        throw new Error('Failed to send message')
      }

      const data: ChatResponse = await response.json()
      setConversationId(data.conversation_id)

      // Poll for completion
      let status = data.status
      let finalData = data
      let attempts = 0
      const maxAttempts = 60 // 2 minutes with 2s interval

      while (status !== 'COMPLETED' && status !== 'FAILED' && attempts < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, 2000))
        attempts++

        const statusResponse = await fetch(
          `/api/chat/${data.conversation_id}/${data.message_id}/status`
        )

        if (!statusResponse.ok) {
          throw new Error('Failed to check status')
        }

        finalData = await statusResponse.json()
        status = finalData.status

        // Update message with intermediate status
        if (status !== 'COMPLETED' && status !== 'FAILED') {
          setMessages(prev =>
            prev.map(m =>
              m.id === assistantId
                ? { ...m, content: `Processing... (${attempts * 2}s)` }
                : m
            )
          )
        }
      }

      // Update with final response
      setMessages(prev =>
        prev.map(m =>
          m.id === assistantId
            ? {
                ...m,
                content: finalData.content || 'No response received',
                query: finalData.query,
                results: finalData.results,
                status: status === 'COMPLETED' ? 'completed' : 'failed',
              }
            : m
        )
      )

      if (status === 'FAILED') {
        setError(finalData.error || 'Query failed')
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Unknown error'
      setError(errorMessage)

      setMessages(prev =>
        prev.map(m =>
          m.id === assistantId
            ? {
                ...m,
                content: `Error: ${errorMessage}`,
                status: 'failed',
              }
            : m
        )
      )
    } finally {
      setLoading(false)
    }
  }, [conversationId])

  const clearConversation = useCallback(() => {
    setMessages([])
    setConversationId(null)
    setError(null)
  }, [])

  return {
    messages,
    loading,
    error,
    sendMessage,
    clearConversation,
  }
}
