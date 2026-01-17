import { NextResponse, type NextRequest } from 'next/server'
import { updateSession } from '@/lib/supabase/middleware'
import { createServerClient } from '@supabase/ssr'

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!

// Runtime check for environment variables
if (!process.env.NEXT_PUBLIC_SUPABASE_URL || !process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY) {
  throw new Error(
    'Missing Supabase environment variables in middleware. ' +
    'Please ensure NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY are set.'
  )
}

// In-memory store for middleware rate limiting (Edge-compatible)
// Note: This is per-instance, but effective for basic protection on custom servers
const rateLimitStore = new Map<string, { count: number; resetTime: number }>();
const MAX_RATE_LIMIT_ENTRIES = 10000;
let lastCleanup = Date.now();
const CLEANUP_INTERVAL = 60000; // 1 minute

function cleanupExpiredEntries() {
  const now = Date.now();
  if (now - lastCleanup < CLEANUP_INTERVAL) return;
  
  lastCleanup = now;
  for (const [key, value] of rateLimitStore) {
    if (now > value.resetTime) {
      rateLimitStore.delete(key);
    }
  }
  
  // Emergency cleanup if still too large (evict oldest)
  if (rateLimitStore.size > MAX_RATE_LIMIT_ENTRIES) {
    const entries = Array.from(rateLimitStore.entries());
    entries.sort((a, b) => a[1].resetTime - b[1].resetTime);
    const toDelete = entries.slice(0, entries.length - MAX_RATE_LIMIT_ENTRIES);
    for (const [key] of toDelete) {
      rateLimitStore.delete(key);
    }
  }
}

function checkRateLimit(key: string, limit: number, windowMs: number) {
  cleanupExpiredEntries();
  
  const now = Date.now();
  const record = rateLimitStore.get(key);

  if (!record || now > record.resetTime) {
    rateLimitStore.set(key, { count: 1, resetTime: now + windowMs });
    return { allowed: true, remaining: limit - 1, reset: now + windowMs, limit };
  }

  record.count++;
  return {
    allowed: record.count <= limit,
    remaining: Math.max(0, limit - record.count),
    reset: record.resetTime,
    limit
  };
}

export async function middleware(request: NextRequest) {
  const response = await updateSession(request)
  const pathname = request.nextUrl.pathname;

  // 1. Determine Tier and Check Rate Limit
  if (pathname.startsWith('/api')) {
    const ip = request.headers.get("x-real-ip") || request.headers.get("x-forwarded-for")?.split(',')[0] || "127.0.0.1";
    
    // Auth Check (Redundant but necessary for tiered limiting in middleware)
      const supabase = createServerClient(
        supabaseUrl,
        supabaseAnonKey,
      {
        cookies: {
          getAll() { return request.cookies.getAll() },
          setAll() {} // Read-only in this check
        },
      }
    )
    const { data: { user } } = await supabase.auth.getUser()

    let tier = 'public';
    let limit = 30;
    const windowMs = 60000;

    if (pathname.startsWith('/api/auth')) {
      tier = 'auth';
      limit = 10; // Slightly more generous than 5 to account for UI retries
    } else if (user) {
      tier = 'authenticated';
      limit = 120;
    }

    const rl = checkRateLimit(`${tier}:${ip}`, limit, windowMs);

    // Set Rate Limit Headers
    response.headers.set('X-RateLimit-Limit', rl.limit.toString());
    response.headers.set('X-RateLimit-Remaining', rl.remaining.toString());
    response.headers.set('X-RateLimit-Reset', rl.reset.toString());

    if (!rl.allowed) {
      return new NextResponse(
        JSON.stringify({ 
          error: 'Too many requests. Please wait a moment.',
          code: 'RATE_LIMITED',
          retryAfter: Math.ceil((rl.reset - Date.now()) / 1000)
        }),
        { 
          status: 429, 
          headers: { 
            'Content-Type': 'application/json',
            'X-RateLimit-Limit': rl.limit.toString(),
            'X-RateLimit-Remaining': '0',
            'X-RateLimit-Reset': rl.reset.toString(),
            'Retry-After': Math.ceil((rl.reset - Date.now()) / 1000).toString()
          } 
        }
      );
    }
  }

  // 2. Add Security Headers
  response.headers.set('X-Frame-Options', 'DENY')
  response.headers.set('X-Content-Type-Options', 'nosniff')
  response.headers.set('X-XSS-Protection', '1; mode=block')
  response.headers.set('X-Permitted-Cross-Domain-Policies', 'none')
  response.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin')
  response.headers.set(
    'Permissions-Policy',
    'camera=(), microphone=(), geolocation=(), interest-cohort=(), payment=()'
  )
  response.headers.set(
    'Content-Security-Policy',
    "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' https://*.supabase.co; style-src 'self' 'unsafe-inline'; img-src 'self' blob: data: https://*.supabase.co https://*.unsplash.com https://*.mangadex.org https://*.mangapark.net https://*.mangasee123.com https://*.manga4life.com; font-src 'self' data:; connect-src 'self' https://*.supabase.co https://*.mangadex.org; worker-src 'self' blob:; frame-src 'none'; frame-ancestors 'none'; upgrade-insecure-requests;"
  )

  if (process.env.NODE_ENV === 'production') {
    response.headers.set(
      'Strict-Transport-Security',
      'max-age=31536000; includeSubDomains'
    )
  }

  return response
}

export const config = {
  matcher: [
    /*
     * Match all request paths except for the ones starting with:
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     * Feel free to modify this pattern to include more paths.
     */
    '/((?!_next/static|_next/image|favicon.ico|.*\\.(?:svg|png|jpg|jpeg|gif|webp)$).*)',
  ],
}
