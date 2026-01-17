describe('Auth API', () => {
  const BASE_URL = process.env.TEST_BASE_URL || 'http://localhost:3000'

  describe('GET /api/auth/check-username', () => {
    it('should return 400 without username param', async () => {
      const response = await fetch(`${BASE_URL}/api/auth/check-username`)
      expect(response.status).toBe(400)
    })

    it('should return availability for valid username', async () => {
      const response = await fetch(`${BASE_URL}/api/auth/check-username?username=testuser12345`)
      expect(response.status).toBe(200)
      
      const data = await response.json()
      expect(data).toHaveProperty('available')
      expect(typeof data.available).toBe('boolean')
    })

    it('should reject usernames with invalid characters', async () => {
      const response = await fetch(`${BASE_URL}/api/auth/check-username?username=test@user!`)
      expect(response.status).toBe(400)
    })

    it('should reject too short usernames', async () => {
      const response = await fetch(`${BASE_URL}/api/auth/check-username?username=ab`)
      expect(response.status).toBe(400)
    })

    it('should reject too long usernames', async () => {
      const longUsername = 'a'.repeat(51)
      const response = await fetch(`${BASE_URL}/api/auth/check-username?username=${longUsername}`)
      expect(response.status).toBe(400)
    })
  })
})
