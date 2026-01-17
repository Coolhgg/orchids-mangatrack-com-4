import { antiAbuse } from '@/lib/anti-abuse'

describe('Anti-Abuse System', () => {
  const testUserId = '550e8400-e29b-41d4-a716-446655440000'
  const testEntryId = '660e8400-e29b-41d4-a716-446655440000'

  describe('detectProgressBotPatterns', () => {
    it('should allow normal progress', async () => {
      const result = await antiAbuse.detectProgressBotPatterns(
        testUserId,
        testEntryId,
        10,
        5
      )
      expect(result.isBot).toBe(false)
    })

    it('should flag massive chapter jumps', async () => {
      const result = await antiAbuse.detectProgressBotPatterns(
        testUserId,
        testEntryId,
        1000,
        1
      )
      expect(result.isBot).toBe(true)
      expect(result.reason).toBeDefined()
    })
  })

  describe('checkProgressRateLimit', () => {
    it('should return rate limit status', async () => {
      const result = await antiAbuse.checkProgressRateLimit(testUserId)
      expect(typeof result.allowed).toBe('boolean')
    })
  })

  describe('canGrantXp', () => {
    it('should return XP grant permission', async () => {
      const result = await antiAbuse.canGrantXp(testUserId)
      expect(typeof result).toBe('boolean')
    })
  })
})
