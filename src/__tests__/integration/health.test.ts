describe('Health Check API', () => {
  const BASE_URL = process.env.TEST_BASE_URL || 'http://localhost:3000'

  describe('GET /api/health', () => {
    it('should return 200 with health status', async () => {
      const response = await fetch(`${BASE_URL}/api/health`)
      expect(response.status).toBe(200)
      
      const data = await response.json()
      expect(data).toHaveProperty('status')
      expect(['healthy', 'degraded', 'unhealthy']).toContain(data.status)
    })

    it('should include database connectivity info', async () => {
      const response = await fetch(`${BASE_URL}/api/health`)
      const data = await response.json()
      
      expect(data).toHaveProperty('database')
      expect(typeof data.database).toBe('object')
    })

    it('should include timestamp', async () => {
      const response = await fetch(`${BASE_URL}/api/health`)
      const data = await response.json()
      
      expect(data).toHaveProperty('timestamp')
    })
  })
})
