import { supabaseAdmin } from "@/lib/supabase/admin"
import { NextRequest, NextResponse } from "next/server"
import { getRateLimitInfo, handleApiError, getClientIp, ErrorCodes } from "@/lib/api-utils"
import { getBestCoversBatch, isValidCoverUrl } from "@/lib/cover-resolver"

const VALID_MODES = ['velocity', 'classic'] as const
const VALID_TYPES = ['manga', 'manhwa', 'manhua', 'webtoon'] as const

type Mode = typeof VALID_MODES[number]

export async function GET(request: NextRequest) {
  const ip = getClientIp(request);
  const rateLimitInfo = await getRateLimitInfo(`trending:${ip}`, 60, 60000)
  
  if (!rateLimitInfo.allowed) {
    const retryAfter = Math.ceil((rateLimitInfo.reset - Date.now()) / 1000)
    return NextResponse.json(
      { error: 'Too many requests. Please wait a moment.', code: ErrorCodes.RATE_LIMITED },
      { 
        status: 429,
        headers: { 'Retry-After': retryAfter.toString() }
      }
    )
  }

  const searchParams = request.nextUrl.searchParams
  const mode = (searchParams.get('mode') || 'velocity') as Mode
  const type = searchParams.get('type')
  const limit = Math.min(Math.max(1, parseInt(searchParams.get('limit') || '20')), 50)
  const offset = Math.max(0, parseInt(searchParams.get('offset') || '0'))

  if (!VALID_MODES.includes(mode)) {
    return NextResponse.json(
      { error: 'Invalid mode. Must be one of: velocity, classic' },
      { status: 400 }
    )
  }

    try {
      const { data: trendingData, error, count } = await supabaseAdmin.rpc('get_velocity_trending_series', {
        p_type: type || null,
        p_limit: limit,
        p_offset: offset
      }, { count: 'exact' })

      if (error) throw error

      const seriesIds = (trendingData || []).map((s: any) => s.id)
      const bestCovers = await getBestCoversBatch(seriesIds)

      return NextResponse.json({
        results: (trendingData || []).map((s: any) => {
          const bestCover = bestCovers.get(s.id)
          const fallbackCover = isValidCoverUrl(s.cover_url) ? s.cover_url : null
          return {
            id: s.id,
            title: s.title,
            cover_url: bestCover?.cover_url || fallbackCover,
            type: s.type,
            status: s.status,
            total_follows: s.total_follows,
            latest_chapter: s.latest_chapter,
            last_chapter_at: s.last_chapter_at,
            trending_score: s.trending_score,
            velocity: {
              chapters: s.v_chapters,
              follows: s.v_follows,
              activity: s.v_activity,
              chapters_24h: s.chapters_24h,
              chapters_72h: s.chapters_72h,
              follows_24h: s.follows_24h,
              follows_72h: s.follows_72h,
              last_chapter_event_at: s.last_chapter_event_at
            }
          }
        }),
        total: count || (trendingData || []).length,
        limit,
        offset,
        mode,
        has_more: (trendingData || []).length === limit
      })


  } catch (error: any) {
    return handleApiError(error)
  }
}
