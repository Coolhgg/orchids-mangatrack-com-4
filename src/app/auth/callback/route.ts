import { NextResponse } from 'next/server'
// The client you created from the Server-Side Auth instructions
import { createClient } from '@/lib/supabase/server'
import { checkRateLimit, getClientIp, getSafeRedirect } from "@/lib/api-utils"
import { prisma } from "@/lib/prisma"

export async function GET(request: Request) {
  const { searchParams, origin } = new URL(request.url)
  const code = searchParams.get('code')
  // if "next" is in search params, use it as the redirection URL
  const next = getSafeRedirect(searchParams.get('next'), '/library')

  // SECURITY: Rate limit OAuth callback to prevent code brute-forcing (BUG 18)
  const ip = getClientIp(request)
  if (!await checkRateLimit(`oauth:${ip}`, 10, 60000)) {
    return NextResponse.redirect(`${origin}/auth/auth-code-error?error=rate_limited`)
  }

  if (code) {
    const supabase = await createClient()

    // BUG 77: Session fixation protection
    // Ensure we don't have an existing stale session before exchanging the code
    await supabase.auth.signOut();

    const { data, error } = await supabase.auth.exchangeCodeForSession(code)
    if (!error && data.user) {
      // BUG-003 FIX: Check if user is soft-deleted before allowing login
      try {
        const dbUser = await prisma.$queryRaw<{ deleted_at: Date | null }[]>`
          SELECT deleted_at FROM "User" WHERE id = ${data.user.id}::uuid LIMIT 1
        `
        
        if (dbUser.length > 0 && dbUser[0].deleted_at !== null) {
          // User is soft-deleted, sign them out and redirect to error
          await supabase.auth.signOut()
          return NextResponse.redirect(`${origin}/auth/auth-code-error?error=account_deleted`)
        }
      } catch (dbError) {
        // If DB check fails, allow login but log the error
        // User might be new and not yet in the database
        console.warn('[Auth] Could not verify soft-delete status:', dbError)
      }
      
      const forwardedHost = request.headers.get('x-forwarded-host')
      const isLocalEnv = process.env.NODE_ENV === 'development'
      
      if (isLocalEnv) {
        return NextResponse.redirect(`${origin}${next}`)
      } else if (forwardedHost) {
        return NextResponse.redirect(`https://${forwardedHost}${next}`)
      } else {
        return NextResponse.redirect(`${origin}${next}`)
      }
    }
  }

  // return the user to an error page with instructions
  return NextResponse.redirect(`${origin}/auth/auth-code-error`)
}
