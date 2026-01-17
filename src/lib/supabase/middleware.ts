import { createServerClient } from '@supabase/ssr'
import { NextResponse, type NextRequest } from 'next/server'

export async function updateSession(request: NextRequest) {
  let supabaseResponse = NextResponse.next({
    request,
  })

  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
      {
        cookies: {
          getAll() {
            return request.cookies.getAll()
          },
          setAll(cookiesToSet) {
            cookiesToSet.forEach(({ name, value }) => request.cookies.set(name, value))
            supabaseResponse = NextResponse.next({
              request,
            })
            cookiesToSet.forEach(({ name, value, options }) =>
              supabaseResponse.cookies.set(name, value, options)
            )
          },
        },
      }
  )

  const {
    data: { user },
  } = await supabase.auth.getUser()

  const publicPaths = [
    '/login',
    '/register',
    '/forgot-password',
    '/reset-password',
    '/auth',
    '/onboarding',
  ]

  const publicApiPaths = [
    '/api/health',
    '/api/auth/check-username',
    '/api/auth/lockout',
    '/api/proxy/image',
    '/api/proxy/check-url',
  ]

  const isPublicPath = publicPaths.some(path => 
    request.nextUrl.pathname.startsWith(path)
  )

  const isPublicApiPath = publicApiPaths.some(path =>
    request.nextUrl.pathname.startsWith(path)
  )

  const isApiPath = request.nextUrl.pathname.startsWith('/api')

  if (!user) {
    // For protected API routes, return 401 JSON response
    if (isApiPath && !isPublicApiPath) {
      return new NextResponse(
        JSON.stringify({ error: 'unauthorized' }),
        { status: 401, headers: { 'Content-Type': 'application/json' } }
      )
    }

    // For protected pages, redirect to login
    // BUG FIX: Don't redirect public API paths - they should pass through
    if (!isPublicPath && !isPublicApiPath && request.nextUrl.pathname !== '/') {
      const url = request.nextUrl.clone()
      url.pathname = '/login'
      return NextResponse.redirect(url)
    }
  }

  return supabaseResponse
}
