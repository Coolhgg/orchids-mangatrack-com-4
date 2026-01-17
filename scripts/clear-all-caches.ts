import Redis from 'ioredis';
import 'dotenv/config';

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
const environment = process.env.NODE_ENV || 'development';
const prefixes = [
  `kenmei:${environment}:`,
  'orchid:search:',
  'kenmei:development:',
  'kenmei:production:',
  'kenmei:test:'
];

async function clearAllCaches() {
  const redis = new Redis(redisUrl);
  console.log(`[Cache] Connecting to Redis at ${redisUrl}...`);

  try {
    // 1. Clear Redis keys by prefix
    for (const prefix of prefixes) {
      const pattern = `${prefix}*`;
      console.log(`[Cache] Searching for keys with pattern: ${pattern}`);
      
      let cursor = '0';
      let totalDeleted = 0;
      
      do {
        const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
        cursor = nextCursor;
        
        if (keys.length > 0) {
          await redis.del(...keys);
          totalDeleted += keys.length;
        }
      } while (cursor !== '0');
      
      if (totalDeleted > 0) {
        console.log(`[Cache] Deleted ${totalDeleted} keys for prefix: ${prefix}`);
      } else {
        console.log(`[Cache] No keys found for prefix: ${prefix}`);
      }
    }

    // 2. Optional: If we want to be aggressive and flush the whole DB
    // await redis.flushdb();
    // console.log('[Cache] Database flushed (FLUSHDB)');

  } catch (error) {
    console.error('[Cache] Error clearing Redis cache:', error);
  } finally {
    await redis.quit();
    console.log('[Cache] Redis connection closed.');
  }
}

clearAllCaches().catch(console.error);
