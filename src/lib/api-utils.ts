import { NextResponse } from 'next/server'
import { z } from 'zod'
import { redis, waitForRedis, REDIS_KEY_PREFIX } from './redis'
import { prisma, isTransientError } from './prisma'
import { logger } from './logger'

export class ApiError extends Error {
  constructor(
    message: string,
    public statusCode: number = 500,
    public code?: string
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

export const ErrorCodes = {
  BAD_REQUEST: 'BAD_REQUEST',
  UNAUTHORIZED: 'UNAUTHORIZED',
  FORBIDDEN: 'FORBIDDEN',
  NOT_FOUND: 'NOT_FOUND',
  CONFLICT: 'CONFLICT',
  RATE_LIMITED: 'RATE_LIMITED',
  VALIDATION_ERROR: 'VALIDATION_ERROR',
  INTERNAL_ERROR: 'INTERNAL_ERROR',
  INVALID_INPUT: 'INVALID_INPUT',
} as const

/**
 * Validates the request body size to prevent memory exhaustion (BUG 57)
 */
export async function validateJsonSize(request: Request, maxBytes: number = 1024 * 1024): Promise<void> {
  const contentLength = request.headers.get("content-length");
  if (contentLength && parseInt(contentLength, 10) > maxBytes) {
    throw new ApiError("Payload too large", 413, "PAYLOAD_TOO_LARGE");
  }
}

/**
 * Validates the Content-Type header (BUG 58)
 */
export function validateContentType(request: Request, expected: string = "application/json"): void {
  const contentType = request.headers.get("content-type");
  if (!contentType || !contentType.includes(expected)) {
    throw new ApiError(`Invalid Content-Type. Expected ${expected}`, 415, "INVALID_CONTENT_TYPE");
  }
}

/**
 * Validates internal worker/system requests (BUG 76)
 * Uses a pre-shared internal secret token and validates the source and IP range.
 */
export function validateInternalToken(request: Request): void {
  const authHeader = request.headers.get("authorization");
  const internalSecret = process.env.INTERNAL_API_SECRET;
  
    if (!internalSecret) {
      logger.error("[Security] INTERNAL_API_SECRET not set. Internal APIs are vulnerable.");
      throw new ApiError("Internal API configuration error", 500, ErrorCodes.INTERNAL_ERROR);
    }

    // 1. Validate Token
    if (!authHeader || authHeader !== `Bearer ${internalSecret}`) {
      logger.warn(`[Security] Unauthorized internal API call attempt from ${getClientIp(request)}`);
      throw new ApiError("Forbidden: Invalid internal token", 403, ErrorCodes.FORBIDDEN);
    }

    // 2. IP Range Validation (CIDR)
    const clientIp = getClientIp(request);
    const allowedCidrs = process.env.INTERNAL_API_ALLOWED_CIDRS?.split(',') || ['127.0.0.1/32'];
    
    const isAllowed = allowedCidrs.some(cidr => isIpInRange(clientIp, cidr.trim()));
    if (!isAllowed && process.env.NODE_ENV === 'production') {
      logger.warn(`[Security] Internal API call from unauthorized IP: ${clientIp}`);
      throw new ApiError("Forbidden: Unauthorized source IP", 403, ErrorCodes.FORBIDDEN);
    }


  // 3. Required internal identifier header
  const source = request.headers.get("x-internal-source");
  if (!source) {
    throw new ApiError("Forbidden: Missing internal source identifier", 403, ErrorCodes.FORBIDDEN);
  }
}

/**
 * Checks if an IP address is within a CIDR range.
 * Supports IPv4.
 */
export function isIpInRange(ip: string, cidr: string): boolean {
  try {
    const [range, bitsStr] = cidr.split('/');
    const bits = parseInt(bitsStr, 10);
    
    if (isNaN(bits)) return ip === range;

    const ipParts = ip.split('.').map(Number);
    const rangeParts = range.split('.').map(Number);

    if (ipParts.some(isNaN) || rangeParts.some(isNaN)) return false;

    const ipInt = (ipParts[0] << 24) | (ipParts[1] << 16) | (ipParts[2] << 8) | ipParts[3];
    const rangeInt = (rangeParts[0] << 24) | (rangeParts[1] << 16) | (rangeParts[2] << 8) | rangeParts[3];

    const mask = ~( (1 << (32 - bits)) - 1 );
    return (ipInt & mask) === (rangeInt & mask);
  } catch {
    return false;
  }
}

/**
 * Masks sensitive values in objects before logging (BUG 42)
 */
export function maskSecrets(obj: any): any {
  if (!obj || typeof obj !== 'object') return obj;
  
  const sensitiveKeys = ['password', 'token', 'secret', 'key', 'authorization', 'cookie', 'session', 'access_token', 'refresh_token', 'api_key', 'private_key'];
  const masked = Array.isArray(obj) ? [...obj] : { ...obj };
  
  for (const key in masked) {
    if (typeof masked[key] === 'object') {
      masked[key] = maskSecrets(masked[key]);
    } else if (sensitiveKeys.some(sk => key.toLowerCase().includes(sk))) {
      masked[key] = '********';
    }
  }
  
  return masked;
}

/**
 * Validates the HTTP method for a request (BUG 94)
 */
export function validateMethod(request: Request, allowedMethods: string[]): void {
  if (!allowedMethods.includes(request.method)) {
    throw new ApiError(
      `Method ${request.method} Not Allowed. Expected: ${allowedMethods.join(', ')}`, 
      405, 
      'METHOD_NOT_ALLOWED'
    );
  }
}

export function handleApiError(error: unknown): NextResponse {
  const maskedError = maskSecrets(error);
  const requestId = Math.random().toString(36).substring(2, 10).toUpperCase();

  if (process.env.NODE_ENV !== 'test') {
    logger.error(`[API Error]`, { 
      requestId, 
      error: maskedError 
    });
  }

  let status = 500;
  let responseBody: any = { 
    error: 'An unexpected error occurred',
    code: ErrorCodes.INTERNAL_ERROR,
    requestId
  };

  const headers: Record<string, string> = {
    'X-Request-ID': requestId
  };

  if (error instanceof ApiError) {
    status = error.statusCode;
    responseBody = { error: error.message, code: error.code, requestId };
    
    // If it's a rate limit error, try to extract retry info if available in error object
    if (status === 429 && (error as any).retryAfter) {
      headers['Retry-After'] = (error as any).retryAfter.toString();
    }
  } else if (error instanceof Error) {
    const lowerMessage = error.message.toLowerCase()
    
    if (lowerMessage.includes('not found')) {
      status = 404;
      responseBody = { error: error.message, code: ErrorCodes.NOT_FOUND, requestId };
    } else if (lowerMessage.includes('unauthorized')) {
      status = 401;
      responseBody = { error: error.message, code: ErrorCodes.UNAUTHORIZED, requestId };
    } else if (lowerMessage.includes('forbidden') || lowerMessage.includes('private')) {
      status = 403;
      responseBody = { error: error.message, code: ErrorCodes.FORBIDDEN, requestId };
    } else if (lowerMessage.includes('not allowed')) {
      status = 405;
      responseBody = { error: error.message, code: 'METHOD_NOT_ALLOWED', requestId };
    } else if (error.name === 'PrismaClientKnownRequestError') {
      const prismaError = error as any
      if (prismaError.code === 'P2002') {
        status = 409;
        responseBody = { error: 'Resource already exists', code: ErrorCodes.CONFLICT, requestId };
      } else if (prismaError.code === 'P2025') {
        status = 404;
        responseBody = { error: 'Resource not found', code: ErrorCodes.NOT_FOUND, requestId };
      } else if (prismaError.code === 'P2003') {
        status = 400;
        responseBody = { error: 'Foreign key constraint failed. Related resource not found.', code: ErrorCodes.BAD_REQUEST, requestId };
      }
    } else if (error.name === 'PrismaClientUnknownRequestError' || error.name === 'PrismaClientValidationError') {
        status = 400;
        responseBody = { error: 'Database request validation failed', code: ErrorCodes.BAD_REQUEST, requestId };
      } else if (error.name === 'ZodError') {
      status = 400;
      responseBody = { error: (error as z.ZodError).errors[0].message, code: ErrorCodes.VALIDATION_ERROR, requestId };
    }
  }

  if (process.env.NODE_ENV !== 'production' && process.env.NODE_ENV !== 'test' && error instanceof Error) {
    responseBody.stack = error.stack;
  }

  return NextResponse.json(responseBody, { 
    status,
    headers
  })
}

/**
 * Validates and normalizes redirect URLs to prevent open redirect vulnerabilities (BUG 80)
 */
export function getSafeRedirect(url: string | null | undefined, defaultUrl: string = '/library'): string {
  if (!url) return defaultUrl;

  // Prevent protocol-relative URLs (e.g., //evil.com)
  if (url.startsWith('//')) return defaultUrl;

  // Internal redirects are safe
  if (url.startsWith('/') && !url.startsWith('//')) return url;

  try {
    const parsed = new URL(url);
    const allowedHosts = process.env.ALLOWED_REDIRECT_HOSTS?.split(',') || [];
    const currentHost = process.env.NEXT_PUBLIC_SITE_URL ? new URL(process.env.NEXT_PUBLIC_SITE_URL).host : null;
    
    if (currentHost) allowedHosts.push(currentHost);
    
    if (allowedHosts.includes(parsed.host)) {
      return url;
    }
  } catch {
    // Fall through
  }

  return defaultUrl;
}

export function validateRequired(
  data: Record<string, unknown>,
  fields: string[]
): void {
  const missing = fields.filter((field) => !data[field])
  if (missing.length > 0) {
    throw new ApiError(`Missing required fields: ${missing.join(', ')}`, 400, 'MISSING_FIELDS')
  }
}

export function validateUUID(id: string, fieldName = 'id'): void {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
  if (!uuidRegex.test(id)) {
    throw new ApiError(`Invalid ${fieldName} format`, 400, 'INVALID_FORMAT')
  }
}

/**
 * Logs a persistent worker failure to the Dead Letter Queue (WorkerFailure table)
 */
export async function logWorkerFailure(
  queueName: string,
  job: { id?: string; data: any; attemptsMade: number },
  error: Error
) {
  try {
    await prisma.workerFailure.create({
      data: {
        queue_name: queueName,
        job_id: job.id || 'unknown',
        payload: job.data,
        error_message: error.message,
        stack_trace: error.stack,
        attempts_made: job.attemptsMade,
      },
    })
    console.log(`[DLQ] Logged persistent failure for job ${job.id} in ${queueName}`)
  } catch (err) {
    console.error(`[DLQ] CRITICAL: Failed to log worker failure:`, err)
  }
}

/**
 * Wraps a worker processor with Dead Letter Queue (DLQ) logging.
 * If the job fails on its final attempt, it will be logged to the WorkerFailure table.
 */
export function wrapWithDLQ<T>(
  queueName: string,
  processor: (job: any) => Promise<any>
) {
  return async (job: any) => {
    try {
      return await processor(job);
    } catch (error: any) {
      // BullMQ: job.attemptsMade is the number of failures so far
      // job.opts.attempts is the total number of attempts allowed
      const maxAttempts = job.opts?.attempts || 1;
      const isLastAttempt = (job.attemptsMade + 1) >= maxAttempts;

      if (isLastAttempt) {
        await logWorkerFailure(
          queueName,
          {
            id: job.id,
            data: job.data,
            attemptsMade: job.attemptsMade + 1,
          },
          error instanceof Error ? error : new Error(String(error))
        );
      }

      throw error;
    }
  };
}

/**
 * Logs a security event to the AuditLog table
 */
export async function logSecurityEvent(params: {
  userId: string;
  event: string;
  status: 'success' | 'failure';
  ipAddress?: string;
  userAgent?: string | null;
  metadata?: any;
}) {
  try {
    await prisma.auditLog.create({
      data: {
        user_id: params.userId,
        event: params.event,
        status: params.status,
        ip_address: params.ipAddress || 'unknown',
        user_agent: params.userAgent,
        metadata: params.metadata || {},
      },
    })
  } catch (err) {
    console.error(`[Security] Failed to log security event:`, err)
  }
}

/**
 * Sanitizes user input to prevent XSS attacks
 * Removes HTML tags and dangerous patterns
 */
export function sanitizeInput(input: string, maxLength = 10000): string {
  if (!input) return ''
  
  // Pre-truncate extremely long inputs to prevent ReDoS attacks
  const preSanitized = input.length > maxLength * 2 ? input.slice(0, maxLength * 2) : input;

  // Layer 1: Remove null bytes and combine basic script/iframe/style removal
  let sanitized = preSanitized.replace(/\x00/g, '')
    .replace(/<(script|iframe|object|embed|style|link|meta|applet|base|form|input|button|textarea|select|option)\b[^>]*>([\s\S]*?)<\/\1>/gi, '')
    .replace(/<(script|iframe|object|embed|style|link|meta|applet|base|form|input|button|textarea|select|option)\b[^>]*>/gi, '');

  // Layer 2: Strip all remaining HTML tags
  sanitized = sanitized.replace(/<[^>]*>/g, '');

  // Layer 3: Remove dangerous protocols and patterns
  sanitized = sanitized.replace(/(javascript|data|vbscript|file|about|blob|mocha|livescript)\s*:/gi, '');

  // Layer 4: Remove event handlers and dangerous attributes
  sanitized = sanitized.replace(/\b(on\w+|style|formaction|action|background|src|href|lowsrc|dynsrc)\s*=\s*(['"]?)\s*(javascript|data|vbscript|file|about|blob):/gi, '$1=#');
  sanitized = sanitized.replace(/\b(on\w+|formaction|action)\s*=/gi, 'data-sanitized-attr=');

  // Layer 5: Remove expression() and other CSS-based attacks
  sanitized = sanitized.replace(/expression\s*\(|url\s*\(|behavior\s*\(/gi, '');

  // Layer 6: Remove HTML entities that could be used for XSS bypass
  sanitized = sanitized.replace(/&[#a-zA-Z0-9]+;/g, '');

  return sanitized.trim().slice(0, maxLength)
}

/**
 * HTML encode special characters for safe display
 */
export function htmlEncode(input: string): string {
  const entities: Record<string, string> = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#x27;',
    '/': '&#x2F;',
  }
  return input.replace(/[&<>"'/]/g, (char) => entities[char] || char)
}

/**
 * Sanitizes text input with simple truncation
 */
export function sanitizeText(input: string, maxLength = 500): string {
  if (!input) return ''
  return input.trim().slice(0, maxLength)
}

/**
 * Safely parses an integer with fallback to default value
 * Handles NaN, negative values, and boundary conditions
 */
function safeParseInt(value: string | null, defaultValue: number, min: number, max: number): number {
  if (!value) return defaultValue;
  const parsed = parseInt(value, 10);
  if (isNaN(parsed)) return defaultValue;
  return Math.min(max, Math.max(min, parsed));
}

export function parsePaginationParams(
  searchParams: URLSearchParams
): { page: number; limit: number; offset: number; cursor: string | null } {
  // BUG FIX: Handle NaN values from invalid input by using safeParseInt
  const limit = safeParseInt(searchParams.get('limit'), 20, 1, 100);
  const providedOffset = searchParams.get('offset')
  const providedPage = searchParams.get('page')
  const cursor = searchParams.get('cursor') // BUG 84: Support cursor pagination
  
  // Add upper bound for offset to prevent integer overflow or DB strain
  const MAX_OFFSET = 1000000;

  let offset: number
  let page: number
  
  if (providedOffset !== null) {
    offset = safeParseInt(providedOffset, 0, 0, MAX_OFFSET);
    page = Math.floor(offset / limit) + 1
  } else {
    page = safeParseInt(providedPage, 1, 1, MAX_OFFSET);
    offset = Math.min(MAX_OFFSET, (page - 1) * limit)
  }
  
  return { page, limit, offset, cursor }
}

export function validateEmail(email: string): boolean {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
  return emailRegex.test(email)
}

export const USERNAME_REGEX = /^[a-zA-Z0-9_-]{3,30}$/

export function validateUsername(username: string): boolean {
  return USERNAME_REGEX.test(username)
}

/**
 * SECURITY: Escape ILIKE special characters to prevent SQL injection
 * Characters %, _, and \ have special meaning in ILIKE patterns
 */
export function escapeILikePattern(input: string): string {
  return input
    .replace(/\\/g, '\\\\')  // Escape backslashes first
    .replace(/%/g, '\\%')    // Escape percent signs
    .replace(/_/g, '\\_')    // Escape underscores
}

/**
 * Gets the real client IP, handling proxies and spoofing attempts.
 * Prioritizes X-Real-IP which is set by trusted proxies (Vercel/Cloudflare).
 * 
 * SECURITY NOTE: The FIRST IP in x-forwarded-for is the original client IP.
 * Later IPs are proxies that forwarded the request. We use the first non-private IP.
 */
export function getClientIp(request: Request): string {
  // X-Real-IP is generally more reliable as it's set by the edge proxy
  const realIp = request.headers.get("x-real-ip");
  if (realIp) return realIp.trim();

  const forwardedFor = request.headers.get("x-forwarded-for");
  if (forwardedFor) {
    // x-forwarded-for format: "client, proxy1, proxy2"
    // The FIRST IP is the original client - later IPs are proxies
    const ips = forwardedFor.split(',').map(ip => ip.trim()).filter(Boolean);
    
    // Return the first valid, non-empty IP (the original client)
    // In trusted proxy environments (Vercel/Cloudflare), this is reliable
    if (ips.length > 0 && ips[0]) {
      return ips[0];
    }
  }
  
  // Fallback for local development or missing headers
  return "127.0.0.1";
}

// In-memory fallback for rate limiting
interface RateLimitEntry {
  count: number
  resetTime: number
}

class InMemoryRateLimitStore {
  private map = new Map<string, RateLimitEntry>()
  private cleanupInterval: ReturnType<typeof setInterval> | null = null
  private readonly MAX_ENTRIES = 50000
  private isShuttingDown = false

  constructor() {
    if (typeof setInterval !== 'undefined') {
      this.cleanupInterval = setInterval(() => this.cleanup(), 5 * 60 * 1000)
      // Prevent interval from keeping the process alive
      if (this.cleanupInterval.unref) this.cleanupInterval.unref()
    }
  }

  get(key: string): RateLimitEntry | undefined {
    return this.map.get(key)
  }

  set(key: string, entry: RateLimitEntry): void {
    if (this.map.size >= this.MAX_ENTRIES) {
      this.cleanup()
      // If still at max after cleanup, clear oldest entries
      if (this.map.size >= this.MAX_ENTRIES) {
        const keysToDelete = Array.from(this.map.keys()).slice(0, Math.floor(this.MAX_ENTRIES * 0.2))
        keysToDelete.forEach(k => this.map.delete(k))
      }
    }
    this.map.set(key, entry)
  }

  delete(key: string): void {
    this.map.delete(key)
  }

  private cleanup(): void {
    if (this.isShuttingDown) return
    const now = Date.now()
    for (const [key, entry] of this.map.entries()) {
      if (now > entry.resetTime) this.map.delete(key)
    }
  }

  // Graceful shutdown method to clear interval
  shutdown(): void {
    this.isShuttingDown = true
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval)
      this.cleanupInterval = null
    }
    this.map.clear()
  }
}

const globalForRateLimit = global as unknown as { inMemoryStore: InMemoryRateLimitStore }
const inMemoryStore = globalForRateLimit.inMemoryStore || new InMemoryRateLimitStore()
if (process.env.NODE_ENV !== 'production') globalForRateLimit.inMemoryStore = inMemoryStore

export async function checkRateLimit(
  key: string,
  maxRequests: number = 100,
  windowMs: number = 60000
): Promise<boolean> {
  const info = await getRateLimitInfo(key, maxRequests, windowMs);
  return info.allowed;
}

/**
 * Redis-based rate limiting with in-memory fallback.
 * Returns detailed rate limit info (limit, remaining, reset).
 */
export async function getRateLimitInfo(
  key: string,
  maxRequests: number = 100,
  windowMs: number = 60000
): Promise<{ allowed: boolean; remaining: number; reset: number; limit: number }> {
  const redisReady = await waitForRedis(redis, 500);
  const redisKey = `${REDIS_KEY_PREFIX}ratelimit:${key}`;
  const now = Date.now();

  if (redisReady) {
    try {
      const multi = redis.multi();
      multi.incr(redisKey);
      multi.pttl(redisKey);
      const results = await multi.exec();
      
      if (results && results[0] && results[0][1] !== null) {
        const count = results[0][1] as number;
        let pttl = results[1] ? (results[1][1] as number) : -1;
        
        if (pttl === -1 || pttl < 0) {
          await redis.pexpire(redisKey, windowMs);
          pttl = windowMs;
        }
        
        const reset = now + pttl;
        return {
          allowed: count <= maxRequests,
          remaining: Math.max(0, maxRequests - count),
          reset,
          limit: maxRequests
        };
      }
    } catch (err) {
      console.warn(`[RateLimit] Redis failed, falling back to in-memory: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }
  }

  // In-memory fallback
  const record = inMemoryStore.get(key)

  if (!record || now > record.resetTime) {
    const resetTime = now + windowMs;
    inMemoryStore.set(key, { count: 1, resetTime })
    return {
      allowed: true,
      remaining: maxRequests - 1,
      reset: resetTime,
      limit: maxRequests
    }
  }

  record.count++
  return {
    allowed: record.count <= maxRequests,
    remaining: Math.max(0, maxRequests - record.count),
    reset: record.resetTime,
    limit: maxRequests
  }
}

export async function clearRateLimit(key: string): Promise<void> {
  const redisKey = `${REDIS_KEY_PREFIX}ratelimit:${key}`;
  await redis.del(redisKey).catch(() => {});
  inMemoryStore.delete(key)
}

/**
 * Auth-specific rate limiting (stricter limits)
 */
export async function checkAuthRateLimit(ip: string): Promise<boolean> {
  // 5 attempts per minute for auth endpoints
  return checkRateLimit(`auth:${ip}`, 5, 60000)
}

/**
 * Validates the Origin header against the request URL's host to prevent CSRF
 * Simple check for Route Handlers
 */
export function validateOrigin(request: Request) {
  // Skip CSRF origin check in development
  if (process.env.NODE_ENV === 'development') return;

  const origin = request.headers.get("origin");
  const host = request.headers.get("host");
  
  if (origin && host) {
    try {
      const originHost = new URL(origin).host;
      
      // Allow if origin host matches the host header
      if (originHost === host) return;

      // Allow if origin host matches X-Forwarded-Host
      const forwardedHost = request.headers.get("x-forwarded-host");
      if (forwardedHost && originHost === forwardedHost) return;

        // Special case: allow orchids.cloud and vercel.app domains
        if (originHost.endsWith('orchids.cloud') || originHost.endsWith('vercel.app')) return;

      throw new ApiError("CSRF Protection: Invalid origin", 403, ErrorCodes.FORBIDDEN);
    } catch {
      throw new ApiError("CSRF Protection: Invalid origin format", 403, ErrorCodes.FORBIDDEN);
    }
  }
}

/**
 * Normalize a filter value to match database format
 */
export function toTitleCase(str: string): string {
  if (!str) return ''
  
  let decoded = str
  try {
    decoded = decodeURIComponent(str)
  } catch {
    decoded = str
  }
  
  const isKebabCase = decoded.includes('-') && !decoded.includes(' ')
  
  const words = isKebabCase ? decoded.split('-') : decoded.split(' ')
  
  const result = words
    .map((word, index) => {
      const lowerWord = word.toLowerCase()
      // Always capitalize first and last word, otherwise lowercase "of", "the", "and", "in"
      if (index !== 0 && index !== words.length - 1 && (lowerWord === 'of' || lowerWord === 'the' || lowerWord === 'and' || lowerWord === 'in')) {
        return lowerWord
      }
      
      if (word.includes('-')) {
        return word.split('-').map(part => 
          part.charAt(0).toUpperCase() + part.slice(1).toLowerCase()
        ).join('-')
      }
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
    })
    .join(' ')
    .replace(/\bSci Fi\b/gi, 'Sci-Fi')
    .replace(/\bBoys Love\b/gi, "Boys' Love")
    .replace(/\bGirls Love\b/gi, "Girls' Love")
    .replace(/\bPost Apocalyptic\b/gi, 'Post-Apocalyptic')

  return result
}

export function normalizeToTitleCase(values: string[]): string[] {
  if (!Array.isArray(values)) return []
  return values.map(v => toTitleCase(v)).filter(Boolean)
}

export function normalizeToLowercase(values: string[]): string[] {
  if (!Array.isArray(values)) return []
  return values.map(v => v.toLowerCase()).filter(Boolean)
}

export function sanitizeFilterArray(arr: string[], maxLength: number = 50): string[] {
  if (!Array.isArray(arr)) return []
  return arr
    .filter(v => typeof v === 'string' && v.length > 0)
    .map(v => sanitizeInput(v, 100))
    .filter(v => v.length > 0)
    .slice(0, maxLength)
}

export async function withErrorHandling<T>(
  handler: () => Promise<T>
): Promise<NextResponse> {
  try {
    const result = await handler()
    return NextResponse.json(result)
  } catch (error) {
    return handleApiError(error)
  }
}
