import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@/lib/supabase/server';
import { prisma, DEFAULT_TX_OPTIONS } from '@/lib/prisma';
import { Prisma } from '@prisma/client';
import { logActivity } from '@/lib/gamification/activity';
import { XP_SERIES_COMPLETED, calculateLevel } from '@/lib/gamification/xp';
import { checkAchievements, UnlockedAchievement } from '@/lib/gamification/achievements';
import { validateUUID, handleApiError, ApiError, validateOrigin, ErrorCodes, getClientIp, validateContentType, validateJsonSize, checkRateLimit } from '@/lib/api-utils';
import { recordSignal } from '@/lib/analytics/signals';
import { antiAbuse } from '@/lib/anti-abuse';

/**
 * PATCH /api/library/[id]
 * Updates a library entry status or rating
 * 
 * RESPONSE CONTRACT:
 * - xpGained: base XP only (NOT including achievement XP)
 * - achievements: array of unlocked achievements (empty if none)
 */
export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    // CSRF Protection
    validateOrigin(req);

    // BUG FIX: Validate Content-Type
    validateContentType(req);

    // BUG FIX: Validate JSON Size
    await validateJsonSize(req);

      const supabase = await createClient();
      const { data: { user } } = await supabase.auth.getUser();

      if (!user) {
        throw new ApiError('Unauthorized', 401, ErrorCodes.UNAUTHORIZED);
      }

      const rateCheck = await antiAbuse.checkStatusRateLimit(user.id);
      if (!rateCheck.allowed) {
        throw new ApiError('Too many requests. Please wait a moment.', 429, ErrorCodes.RATE_LIMITED);
      }

    const { id: entryId } = await params;
    
    // Validate UUID format
    validateUUID(entryId, 'entryId');

    let body;
    try {
      body = await req.json();
    } catch {
      throw new ApiError('Invalid JSON body', 400, ErrorCodes.BAD_REQUEST);
    }
    
    const { status, rating, preferred_source } = body;

    // Validate status if provided
    if (status) {
      const validStatuses = ['reading', 'completed', 'planning', 'dropped', 'paused'];
      if (!validStatuses.includes(status)) {
        throw new ApiError('Invalid status', 400, ErrorCodes.VALIDATION_ERROR);
      }
    }

    // Validate rating if provided
    if (rating !== undefined && rating !== null) {
      const ratingNum = Number(rating);
      if (isNaN(ratingNum) || ratingNum < 1 || ratingNum > 10) {
        throw new ApiError('Rating must be between 1 and 10', 400, ErrorCodes.VALIDATION_ERROR);
      }
    }

    // Validate preferred_source if provided
    if (preferred_source !== undefined && preferred_source !== null) {
      if (typeof preferred_source !== 'string' || preferred_source.length > 50) {
        throw new ApiError('Invalid preferred source', 400, ErrorCodes.VALIDATION_ERROR);
      }
    }

      const result = await prisma.$transaction(async (tx) => {
        // 1. Get current entry
        const currentEntry = await tx.libraryEntry.findUnique({
          where: { id: entryId, user_id: user.id },
        });

        if (!currentEntry) {
          throw new ApiError('Library entry not found', 404, ErrorCodes.NOT_FOUND);
        }

        // Bot detection for status toggle abuse (soft block XP only)
        let botDetected = false;
        if (status && status !== currentEntry.status) {
          const botCheck = await antiAbuse.detectStatusBotPatterns(user.id, entryId, status);
          botDetected = botCheck.isBot;
        }

        // XP rate limit guard (5 XP grants per minute)
        const xpAllowed = status === 'completed' ? await antiAbuse.canGrantXp(user.id) : true;

        // 2. Prepare update data
        const updateData: Prisma.LibraryEntryUpdateInput = {};
        if (status) updateData.status = status;
        if (rating !== undefined) updateData.user_rating = rating;
        if (preferred_source !== undefined) updateData.preferred_source = preferred_source;

      // 3. Update entry
      const updatedEntry = await tx.libraryEntry.update({
        where: { id: entryId, user_id: user.id },
        data: updateData,
      });

        // 4. Handle side effects if status changed to 'completed'
        let baseXpGained = 0;
        const unlockedAchievements: UnlockedAchievement[] = [];
        
          if (status === 'completed' && currentEntry.status !== 'completed') {
            // SYSTEM FIX: Use immutable series_completion_xp_granted flag to prevent XP farming
            // This flag is NEVER reset, even if status changes back to non-completed
            // ANTI-ABUSE: Also check bot detection and XP rate limit
            if (!currentEntry.series_completion_xp_granted && !botDetected && xpAllowed) {
              // Award XP and set immutable flag
              const userProfile = await tx.user.findUnique({
                where: { id: user.id },
                select: { xp: true },
              });

            baseXpGained = XP_SERIES_COMPLETED;
            const newXp = (userProfile?.xp || 0) + baseXpGained;
            const newLevel = calculateLevel(newXp);

            await tx.user.update({
              where: { id: user.id },
              data: {
                xp: newXp,
                level: newLevel,
              },
            });

            // Set immutable XP flag - this can NEVER be reset
            await tx.libraryEntry.update({
              where: { id: entryId },
              data: { series_completion_xp_granted: true },
            });

            // Log activity
            await logActivity(tx, user.id, 'series_completed', {
              seriesId: currentEntry.series_id ?? undefined,
            });

            // Check achievements (XP awarded internally)
            try {
              const achievements = await checkAchievements(tx, user.id, 'series_completed');
              unlockedAchievements.push(...achievements);
            } catch (achievementError) {
              console.error('Failed to check achievements:', achievementError);
            }
          }
        } else if (status && status !== currentEntry.status) {
          // Log status update activity
          await logActivity(tx, user.id, 'status_updated', {
            seriesId: currentEntry.series_id ?? undefined,
            metadata: { old_status: currentEntry.status, new_status: status },
          });
        }

        return { 
          entry: updatedEntry, 
          seriesId: currentEntry.series_id, 
          baseXpGained,
          unlockedAchievements,
        };
      }, DEFAULT_TX_OPTIONS);

    // Record rating signal outside transaction (non-blocking)
    if (rating !== undefined && rating !== null && result.seriesId) {
      recordSignal({
        user_id: user.id,
        series_id: result.seriesId,
        signal_type: 'rating',
        metadata: { rating: Number(rating) }
      }).catch(err => console.error('[Library] Failed to record rating signal:', err.message));
    }

    // Build response following mandatory contract
    const response: Record<string, unknown> = { ...result.entry };
    
    // Only include xpGained and achievements if XP was awarded
    if (result.baseXpGained > 0) {
      response.xpGained = result.baseXpGained;
      response.achievements = result.unlockedAchievements.map(a => ({
        code: a.code,
        name: a.name,
        xp_reward: a.xp_reward,
        rarity: a.rarity,
      }));
    }

    return NextResponse.json(response);
  } catch (error: any) {
    return handleApiError(error);
  }
}

/**
 * DELETE /api/library/[id]
 * Removes a series from the user's library
 */
export async function DELETE(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    // CSRF Protection
    validateOrigin(req);

    // Rate limit: 30 deletes per minute per IP
    const ip = getClientIp(req);
    if (!await checkRateLimit(`library-delete:${ip}`, 30, 60000)) {
      throw new ApiError('Too many requests. Please wait a moment.', 429, ErrorCodes.RATE_LIMITED);
    }

    const supabase = await createClient();
    const { data: { user } } = await supabase.auth.getUser();

    if (!user) {
      throw new ApiError('Unauthorized', 401, ErrorCodes.UNAUTHORIZED);
    }

    const { id: entryId } = await params;
    
    // Validate UUID format
    validateUUID(entryId, 'entryId');

      const deletedEntry = await prisma.$transaction(async (tx) => {
        const entry = await tx.libraryEntry.findUnique({
          where: { id: entryId, user_id: user.id },
          select: { series_id: true, deleted_at: true },
        });

        if (!entry || entry.deleted_at) {
          throw new ApiError('Library entry not found', 404, ErrorCodes.NOT_FOUND);
        }

        // 1. Soft delete entry
        await tx.libraryEntry.update({
          where: { id: entryId, user_id: user.id },
          data: { deleted_at: new Date() },
        });

        // 2. Log activity
          await logActivity(tx, user.id, 'library_removed', {
            seriesId: entry.series_id ?? undefined,
          });

        // 3. Atomically decrement series follow count (using SQL for floor check)
        if (entry.series_id) {
          await tx.$executeRaw`
            UPDATE series 
            SET total_follows = GREATEST(0, total_follows - 1)
            WHERE id = ${entry.series_id}::uuid
          `;
        }

        return entry;
      }, DEFAULT_TX_OPTIONS);

    // Record remove_from_library signal (non-blocking)
    if (deletedEntry.series_id) {
      recordSignal({
        user_id: user.id,
        series_id: deletedEntry.series_id,
        signal_type: 'remove_from_library',
        metadata: { source: 'library_page' }
      }).catch(err => console.error('[Library] Failed to record remove signal:', err.message));
    }

    return NextResponse.json({ success: true });
  } catch (error: any) {
    return handleApiError(error);
  }
}
