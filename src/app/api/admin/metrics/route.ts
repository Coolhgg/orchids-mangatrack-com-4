import { NextRequest, NextResponse } from "next/server"
import { createClient } from "@/lib/supabase/server"
import { handleApiError, ApiError, ErrorCodes } from "@/lib/api-utils"
import { getMetricsSummary } from "@/lib/metrics"

export async function GET(request: NextRequest) {
  try {
    const supabase = await createClient()
    const { data: { user } } = await supabase.auth.getUser()
    
    if (!user) {
      throw new ApiError("Unauthorized", 401, ErrorCodes.UNAUTHORIZED)
    }

    const metrics = await getMetricsSummary()

    return NextResponse.json({
      timestamp: new Date().toISOString(),
      window_ms: 60000,
      metrics,
    })
  } catch (error) {
    return handleApiError(error)
  }
}
