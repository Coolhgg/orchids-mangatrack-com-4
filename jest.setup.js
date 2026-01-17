import '@testing-library/jest-dom'
import { config } from 'dotenv'

// Load environment variables from .env
config()

// Mock Prisma to avoid browser bundle issue in tests
jest.mock('@/lib/prisma', () => ({
  prisma: {
    $transaction: jest.fn((fn) => fn({
      workerFailure: { create: jest.fn(), findMany: jest.fn().mockResolvedValue([]) },
      auditLog: { create: jest.fn() },
      series: { findUnique: jest.fn(), update: jest.fn(), findMany: jest.fn().mockResolvedValue([]) },
      user: { findUnique: jest.fn(), update: jest.fn() },
      chapter: { upsert: jest.fn(), findMany: jest.fn().mockResolvedValue([]) },
      chapterSource: { upsert: jest.fn() },
      chapter: { upsert: jest.fn(), findMany: jest.fn().mockResolvedValue([]) },
    })),
    workerFailure: { create: jest.fn(), findMany: jest.fn().mockResolvedValue([]) },
    auditLog: { create: jest.fn() },
    series: { findUnique: jest.fn(), update: jest.fn(), findMany: jest.fn().mockResolvedValue([]) },
    user: { findUnique: jest.fn(), update: jest.fn() },
    chapter: { upsert: jest.fn(), findMany: jest.fn().mockResolvedValue([]) },
    chapterSource: { upsert: jest.fn() },
    chapter: { upsert: jest.fn(), findMany: jest.fn().mockResolvedValue([]) },
    $executeRaw: jest.fn(),
    $queryRaw: jest.fn(),
  },
  isTransientError: jest.fn().mockReturnValue(false),
}))

// Ensure we NEVER use the real database in tests unless explicitly requested
if (process.env.TEST_DATABASE_URL) {
  process.env.DATABASE_URL = process.env.TEST_DATABASE_URL
} else if (process.env.NODE_ENV === 'test') {
  // If no test DB provided, use a dummy one or mock it
  process.env.DATABASE_URL = 'postgresql://mock:mock@localhost:5432/mock'
}
if (process.env.TEST_DIRECT_URL) {
  process.env.DIRECT_URL = process.env.TEST_DIRECT_URL
}
if (process.env.TEST_SUPABASE_URL) {
  process.env.SUPABASE_URL = process.env.TEST_SUPABASE_URL
  process.env.NEXT_PUBLIC_SUPABASE_URL = process.env.TEST_SUPABASE_URL
}
if (process.env.TEST_SUPABASE_ANON_KEY) {
  process.env.SUPABASE_ANON_KEY = process.env.TEST_SUPABASE_ANON_KEY
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY = process.env.TEST_SUPABASE_ANON_KEY
}
if (process.env.TEST_SUPABASE_SERVICE_ROLE_KEY) {
  process.env.SUPABASE_SERVICE_ROLE_KEY = process.env.TEST_SUPABASE_SERVICE_ROLE_KEY
}

global.fetch = jest.fn()
global.Request = jest.fn()
global.Response = jest.fn()

jest.mock('next/navigation', () => ({
  useRouter: () => ({
    push: jest.fn(),
    replace: jest.fn(),
    refresh: jest.fn(),
    back: jest.fn(),
    forward: jest.fn(),
  }),
  useSearchParams: () => ({
    get: jest.fn(),
  }),
  usePathname: () => '',
}))
