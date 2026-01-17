import { Job } from 'bullmq';
import { prisma } from '@/lib/prisma';
import { syncSourceQueue, refreshCoverQueue } from '@/lib/queues';
import { SeriesResolutionPayload } from '@/lib/schemas/queue-payloads';
import { searchMangaDex, getMangaById, MangaDexRateLimitError, MangaDexCloudflareError, MangaDexNetworkError } from '@/lib/mangadex';
import { calculateSimilarity, extractPlatformIds } from '@/lib/sync/shared';
import { calculateBackoffWithJitter } from '@/lib/mangadex-utils';

/**
 * Worker processor for Metadata Enrichment.
 * It attempts to find matches on MangaDex for a given LibraryEntry.
 * 
 * STATE SEMANTICS:
 * - pending: Initial state, awaiting enrichment
 * - enriched: Successfully linked to canonical metadata
 * - unavailable: No match found, but entry is healthy (chapters can still sync)
 * - failed: Permanent error during enrichment (should be rare)
 * 
 * KEY DESIGN PRINCIPLE: Metadata is optional. Source health is what matters.
 * An entry with unavailable metadata can still sync chapters perfectly.
 */
export async function processResolution(job: Job<SeriesResolutionPayload>) {
  const { libraryEntryId, source_url, title } = job.data;

  if (!libraryEntryId) {
    console.error('[Enrichment] Missing libraryEntryId in job data');
    return;
  }

  const libEntry = await prisma.libraryEntry.findUnique({
    where: { id: libraryEntryId }
  });

  if (!libEntry || libEntry.metadata_status === 'enriched') {
    return;
  }

  const titleToSearch = title || libEntry.imported_title || '';
  console.log(`[Enrichment] Attempting to enrich metadata for: ${titleToSearch} (URL: ${source_url}) [Attempt ${job.attemptsMade + 1}]`);

  let matchedSeriesId: string | null = null;
  let matchSource: 'mangadex' | null = null;
  let bestCandidate: any = null;
  let maxSimilarity = 0;

  try {
    // 1. EXACT ID MATCH FROM URL (Highest Priority)
    const platformInfo = extractPlatformIds(source_url || libEntry.source_url);
    if (platformInfo) {
      console.log(`[Enrichment] Extracted ${platformInfo.platform} ID: ${platformInfo.id}`);
      if (platformInfo.platform === 'mangadex') {
        bestCandidate = await getMangaById(platformInfo.id);
        if (bestCandidate) {
          matchSource = 'mangadex';
          maxSimilarity = 1.0;
          const existing = await prisma.series.findUnique({ where: { mangadex_id: platformInfo.id } });
          if (existing) matchedSeriesId = existing.id;
        }
      }
    }

    // 2. SEARCH BY TITLE (Fallback)
    if (!matchedSeriesId && titleToSearch) {
      const mdCandidates = await searchMangaDex(titleToSearch);
      
      const REVIEW_THRESHOLD = 0.70;

      const topMd = mdCandidates?.reduce((best, current) => {
        const score = calculateSimilarity(titleToSearch, current.title);
        const bestScore = best ? calculateSimilarity(titleToSearch, best.title) : -1;
        return score > bestScore ? current : best;
      }, null as any);

      if (topMd) {
        const score = calculateSimilarity(titleToSearch, topMd.title);
        if (score >= REVIEW_THRESHOLD) {
          bestCandidate = topMd;
          matchSource = 'mangadex';
          maxSimilarity = score;
          const existing = await prisma.series.findUnique({ where: { mangadex_id: topMd.mangadex_id } });
          if (existing) matchedSeriesId = existing.id;
        }
      }
    }

    // 3. CREATE SERIES IF MATCHED BUT NOT IN DB
    if (!matchedSeriesId && bestCandidate) {
      if (matchSource === 'mangadex') {
        try {
          const series = await prisma.series.upsert({
            where: { mangadex_id: bestCandidate.mangadex_id },
            update: {},
            create: {
              title: bestCandidate.title,
              mangadex_id: bestCandidate.mangadex_id,
              alternative_titles: bestCandidate.alternative_titles,
              description: bestCandidate.description,
              status: bestCandidate.status || "ongoing",
              type: bestCandidate.type || "manga",
              content_rating: bestCandidate.content_rating,
              cover_url: bestCandidate.cover_url,
              external_links: { mangadex: bestCandidate.mangadex_id },
              import_status: 'CANONICALLY_ENRICHED',
              metadata_source: 'CANONICAL',
              metadata_confidence: 1.0,
            }
          });
          matchedSeriesId = series.id;
        } catch (upsertErr: any) {
          // Handle race condition where another worker created it
          if (upsertErr.code === 'P2002') {
            const existing = await prisma.series.findUnique({ 
              where: { mangadex_id: bestCandidate.mangadex_id } 
            });
            if (existing) {
              matchedSeriesId = existing.id;
            } else {
              throw upsertErr;
            }
          } else {
            throw upsertErr;
          }
        }
      }
    }

    // 4. FINALIZE LINKING
    if (matchedSeriesId) {
      const needsReview = maxSimilarity < 0.90;

      await prisma.$transaction(async (tx) => {
        // Re-verify libEntry existence within transaction to avoid race conditions
        const currentEntry = await tx.libraryEntry.findUnique({
          where: { id: libraryEntryId }
        });

        if (!currentEntry || currentEntry.metadata_status === 'enriched') {
          return;
        }

        const existingDuplicate = await tx.libraryEntry.findFirst({
          where: {
            user_id: libEntry.user_id,
            series_id: matchedSeriesId,
            id: { not: libraryEntryId }
          }
        });

        if (existingDuplicate) {
          console.log(`[Enrichment] Found existing duplicate entry ${existingDuplicate.id}. Merging...`);
          const newProgress = Number(libEntry.last_read_chapter || 0);
          const oldProgress = Number(existingDuplicate.last_read_chapter || 0);
          
          if (newProgress > oldProgress) {
            await tx.libraryEntry.update({
              where: { id: existingDuplicate.id },
              data: { 
                last_read_chapter: newProgress,
                updated_at: new Date()
              }
            });
          }
          
          // Delete the redundant entry
          await tx.libraryEntry.delete({ where: { id: libraryEntryId } });
          
          // Link sources to the canonical series
          const entryUrl = source_url || libEntry.source_url;
          if (entryUrl) {
            await tx.seriesSource.updateMany({
              where: { source_url: entryUrl },
              data: { series_id: matchedSeriesId }
            });
          }
          return;
        }

        // Standard update
        await tx.libraryEntry.update({
          where: { id: libraryEntryId },
          data: { 
            series_id: matchedSeriesId, 
            metadata_status: 'enriched',
            needs_review: needsReview,
            metadata_retry_count: 0,
            last_metadata_error: null,
            last_metadata_attempt_at: new Date(),
            updated_at: new Date()
          }
        });

        const entryUrl = source_url || libEntry.source_url;
        if (entryUrl) {
          await tx.seriesSource.updateMany({
            where: { source_url: entryUrl },
            data: { series_id: matchedSeriesId }
          });
        }
      }, {
        isolationLevel: 'Serializable' // Prevent race conditions during merge
      });

      if (matchSource === 'mangadex' && bestCandidate) {
        await refreshCoverQueue.add(`cover-${matchedSeriesId}`, {
          seriesId: matchedSeriesId,
          sourceId: bestCandidate.mangadex_id,
          sourceName: 'mangadex'
        });
      }

      console.log(`[Enrichment] Successfully linked "${titleToSearch}" to ${matchedSeriesId}`);
    } else {
      // NO MATCH FOUND - Use 'unavailable' instead of 'failed'
      // This is NOT a failure - it means metadata simply isn't available on MangaDex
      // The entry is still healthy and chapters can sync normally
      await prisma.libraryEntry.update({
        where: { id: libraryEntryId },
        data: { 
          metadata_status: 'unavailable',
          metadata_retry_count: (libEntry.metadata_retry_count || 0) + 1,
          last_metadata_error: 'No match found on MangaDex',
          last_metadata_attempt_at: new Date()
        }
      });
      console.log(`[Enrichment] No match found for "${titleToSearch}". Marked as unavailable (healthy, no metadata).`);
      
      // DO NOT throw UnrecoverableError - this is a normal outcome, not a failure
      // Background re-resolution will retry these entries weekly
    }
  } catch (err: any) {
    const isRateLimit = err instanceof MangaDexRateLimitError;
    const isCloudflare = err instanceof MangaDexCloudflareError;
    const isNetwork = err instanceof MangaDexNetworkError || err.name === 'AbortError' || err.message.includes('timeout');
    
    const isTransient = isRateLimit || isCloudflare || isNetwork || (err.status && err.status >= 500);

    console.error(`[Enrichment] Error during resolution for "${titleToSearch}":`, err.message);

    if (isTransient) {
      const retryCount = (libEntry.metadata_retry_count || 0) + 1;
      const backoffDelay = calculateBackoffWithJitter(retryCount);
      
      await prisma.libraryEntry.update({
        where: { id: libraryEntryId },
        data: { 
          metadata_retry_count: retryCount,
          last_metadata_error: `${err.name}: ${err.message}`,
          last_metadata_attempt_at: new Date(),
          metadata_status: 'pending' // Ensure it stays pending for retry
        }
      });

      console.log(`[Enrichment] Transient error. Scheduling retry ${retryCount} in ${Math.round(backoffDelay/1000)}s`);
      
      // Rethrow to trigger BullMQ retry with backoff
      throw err;
    }

    // Non-transient errors (e.g. 404, invalid data, schema validation errors)
    // These are rare but can happen - mark as unavailable, not failed
    // The user can manually fix via the Fix Metadata dialog
    await prisma.libraryEntry.update({
      where: { id: libraryEntryId },
      data: { 
        metadata_status: 'unavailable',
        last_metadata_error: err.message,
        last_metadata_attempt_at: new Date()
      }
    });
    
    // DO NOT throw UnrecoverableError
    // Instead, we gracefully handle the error and let background re-resolution retry later
    console.log(`[Enrichment] Non-transient error for "${titleToSearch}". Marked as unavailable (will retry later).`);
  }
}
