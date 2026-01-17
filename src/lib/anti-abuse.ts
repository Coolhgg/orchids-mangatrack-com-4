import { redis, REDIS_KEY_PREFIX, waitForRedis } from './redis';
import { maybeRecordViolation, ViolationType } from './gamification/trust-score';

interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  reset: number;
}

interface BotDetectionResult {
  isBot: boolean;
  reason?: string;
  violationType?: ViolationType;
}

const ABUSE_KEY_PREFIX = `${REDIS_KEY_PREFIX}abuse:`;

class InMemoryAbuseStore {
  private counters = new Map<string, { count: number; resetAt: number }>();
  private lastValues = new Map<string, { value: string; timestamp: number }>();

  increment(key: string, windowMs: number, maxCount: number): RateLimitResult {
    const now = Date.now();
    const record = this.counters.get(key);
    
    if (!record || now > record.resetAt) {
      this.counters.set(key, { count: 1, resetAt: now + windowMs });
      return { allowed: true, remaining: maxCount - 1, reset: now + windowMs };
    }
    
    record.count++;
    return {
      allowed: record.count <= maxCount,
      remaining: Math.max(0, maxCount - record.count),
      reset: record.resetAt,
    };
  }

  getLastValue(key: string): { value: string; timestamp: number } | null {
    const entry = this.lastValues.get(key);
    if (!entry) return null;
    if (Date.now() - entry.timestamp > 60000) {
      this.lastValues.delete(key);
      return null;
    }
    return entry;
  }

  setLastValue(key: string, value: string): void {
    this.lastValues.set(key, { value, timestamp: Date.now() });
  }
}

const globalForAbuse = global as unknown as { abuseStore: InMemoryAbuseStore };
const memoryStore = globalForAbuse.abuseStore || new InMemoryAbuseStore();
if (process.env.NODE_ENV !== 'production') globalForAbuse.abuseStore = memoryStore;

async function rateLimit(
  key: string,
  maxRequests: number,
  windowMs: number
): Promise<RateLimitResult> {
  const redisReady = await waitForRedis(redis, 300);
  const fullKey = `${ABUSE_KEY_PREFIX}${key}`;
  const now = Date.now();

  if (redisReady) {
    try {
      const multi = redis.multi();
      multi.incr(fullKey);
      multi.pttl(fullKey);
      const results = await multi.exec();

      if (results && results[0] && results[0][1] !== null) {
        const count = results[0][1] as number;
        let pttl = results[1] ? (results[1][1] as number) : -1;

        if (pttl === -1 || pttl < 0) {
          await redis.pexpire(fullKey, windowMs);
          pttl = windowMs;
        }

        return {
          allowed: count <= maxRequests,
          remaining: Math.max(0, maxRequests - count),
          reset: now + pttl,
        };
      }
    } catch {
      // Fall through to memory
    }
  }

  return memoryStore.increment(key, windowMs, maxRequests);
}

async function getLastChapter(userId: string, entryId: string): Promise<number | null> {
  const key = `last-chapter:${userId}:${entryId}`;
  const redisReady = await waitForRedis(redis, 200);

  if (redisReady) {
    try {
      const val = await redis.get(`${ABUSE_KEY_PREFIX}${key}`);
      return val ? parseInt(val, 10) : null;
    } catch {
      // Fall through
    }
  }

  const entry = memoryStore.getLastValue(key);
  return entry ? parseInt(entry.value, 10) : null;
}

async function setLastChapter(userId: string, entryId: string, chapter: number): Promise<void> {
  const key = `last-chapter:${userId}:${entryId}`;
  const redisReady = await waitForRedis(redis, 200);

  if (redisReady) {
    try {
      await redis.set(`${ABUSE_KEY_PREFIX}${key}`, String(chapter), 'EX', 60);
      return;
    } catch {
      // Fall through
    }
  }

  memoryStore.setLastValue(key, String(chapter));
}

async function getLastStatus(userId: string, entryId: string): Promise<string | null> {
  const key = `last-status:${userId}:${entryId}`;
  const redisReady = await waitForRedis(redis, 200);

  if (redisReady) {
    try {
      return await redis.get(`${ABUSE_KEY_PREFIX}${key}`);
    } catch {
      // Fall through
    }
  }

  const entry = memoryStore.getLastValue(key);
  return entry?.value || null;
}

async function setLastStatus(userId: string, entryId: string, status: string): Promise<void> {
  const key = `last-status:${userId}:${entryId}`;
  const redisReady = await waitForRedis(redis, 200);

  if (redisReady) {
    try {
      await redis.set(`${ABUSE_KEY_PREFIX}${key}`, status, 'EX', 300);
      return;
    } catch {
      // Fall through
    }
  }

  memoryStore.setLastValue(key, status);
}

export const antiAbuse = {
  /**
   * Check progress rate limit and apply trust score penalty if violated
   */
  async checkProgressRateLimit(userId: string): Promise<{ allowed: boolean; hardBlock: boolean }> {
    const minuteLimit = await rateLimit(`progress:min:${userId}`, 10, 60000);
    if (!minuteLimit.allowed) {
      // TRUST SCORE: Record API spam violation
      await maybeRecordViolation(userId, 'api_spam', { 
        type: 'progress_rate_limit',
        limit: 10,
        window: '1 minute'
      });
      return { allowed: false, hardBlock: true };
    }

    const burstLimit = await rateLimit(`progress:burst:${userId}`, 3, 5000);
    if (!burstLimit.allowed) {
      // TRUST SCORE: Record rapid reads violation
      await maybeRecordViolation(userId, 'rapid_reads', {
        type: 'burst_limit',
        limit: 3,
        window: '5 seconds'
      });
      return { allowed: false, hardBlock: true };
    }

    return { allowed: true, hardBlock: false };
  },

  /**
   * Check status change rate limit and apply trust score penalty if violated
   */
  async checkStatusRateLimit(userId: string): Promise<{ allowed: boolean; hardBlock: boolean }> {
    const minuteLimit = await rateLimit(`status:min:${userId}`, 5, 60000);
    if (!minuteLimit.allowed) {
      // TRUST SCORE: Record API spam violation
      await maybeRecordViolation(userId, 'api_spam', {
        type: 'status_rate_limit',
        limit: 5,
        window: '1 minute'
      });
    }
    return { allowed: minuteLimit.allowed, hardBlock: !minuteLimit.allowed };
  },

  /**
   * Check if XP can be granted (rate limited to prevent farming)
   */
  async canGrantXp(userId: string): Promise<boolean> {
    const result = await rateLimit(`xp:${userId}`, 5, 60000);
    return result.allowed;
  },

  /**
   * Detect bot patterns in progress updates and apply trust score penalties
   */
  async detectProgressBotPatterns(
    userId: string,
    entryId: string,
    chapterNumber: number | null | undefined,
    currentLastRead: number
  ): Promise<BotDetectionResult> {
    if (chapterNumber === null || chapterNumber === undefined) {
      return { isBot: false };
    }

    // Check for repeated same chapter
    const lastChapter = await getLastChapter(userId, entryId);
    if (lastChapter !== null && lastChapter === chapterNumber) {
      // TRUST SCORE: Record repeated same chapter violation
      await maybeRecordViolation(userId, 'repeated_same_chapter', {
        chapter: chapterNumber,
        entryId
      });
      return { isBot: true, reason: 'repeated_same_chapter', violationType: 'repeated_same_chapter' };
    }

      await setLastChapter(userId, entryId, chapterNumber);
    return { isBot: false };
  },

  /**
   * Detect bot patterns in status changes and apply trust score penalties
   */
  async detectStatusBotPatterns(
    userId: string,
    entryId: string,
    newStatus: string
  ): Promise<BotDetectionResult> {
    const lastStatus = await getLastStatus(userId, entryId);
    
    if (lastStatus && lastStatus !== newStatus) {
      const toggleKey = `status-toggle:${userId}:${entryId}`;
      const toggleResult = await rateLimit(toggleKey, 3, 300000);
      
      if (!toggleResult.allowed) {
        // TRUST SCORE: Record status toggle violation
        await maybeRecordViolation(userId, 'status_toggle', {
          from: lastStatus,
          to: newStatus,
          entryId
        });
        return { isBot: true, reason: 'rapid_status_toggle', violationType: 'status_toggle' };
      }
    }

    await setLastStatus(userId, entryId, newStatus);
    return { isBot: false };
  },
};

export type AntiAbuse = typeof antiAbuse;
