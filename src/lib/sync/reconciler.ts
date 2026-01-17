import { SyncAction, SyncOutbox } from './outbox';

const MAX_RETRIES = 5;

export const SyncReconciler = {
  async processOutbox() {
    if (typeof window === 'undefined' || !navigator.onLine) return;

    const actions = SyncOutbox.getActions();
    if (actions.length === 0) return;

    // Sort by timestamp to preserve order of operations
    const sortedActions = [...actions].sort((a, b) => a.timestamp - b.timestamp);

    // 1. Remove actions that have exceeded max retries FIRST
    const expiredActions = sortedActions.filter(a => a.retryCount >= MAX_RETRIES);
    for (const action of expiredActions) {
      console.warn(`Action ${action.id} exceeded max retries (${MAX_RETRIES}), removing from queue.`);
      SyncOutbox.dequeue(action.id);
    }

    // Filter out expired actions for processing
    const validActions = sortedActions.filter(a => a.retryCount < MAX_RETRIES);

    // 2. Group actions for batch processing if possible
    const chapterReadActions = validActions.filter(a => a.type === 'CHAPTER_READ');
    const otherActions = validActions.filter(a => a.type !== 'CHAPTER_READ');

    // 3. Batch replay CHAPTER_READ
    if (chapterReadActions.length > 0) {
      try {
        const response = await fetch('/api/sync/replay', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ actions: chapterReadActions }),
        });

        if (response.ok) {
          const { results } = await response.json();
          for (const res of results) {
            if (res.status === 'success') {
              SyncOutbox.dequeue(res.id);
            } else {
              SyncOutbox.updateRetry(res.id);
            }
          }
        } else {
           chapterReadActions.forEach(a => SyncOutbox.updateRetry(a.id));
        }
      } catch (error) {
        console.error('Batch sync failed:', error);
        chapterReadActions.forEach(a => SyncOutbox.updateRetry(a.id));
      }
    }

    // 4. Process remaining actions sequentially
    for (const action of otherActions) {
      try {
        const success = await this.executeAction(action);
        if (success) {
          SyncOutbox.dequeue(action.id);
        } else {
          SyncOutbox.updateRetry(action.id);
        }
      } catch (error) {
        console.error(`Failed to process action ${action.id}:`, error);
        SyncOutbox.updateRetry(action.id);
      }
    }
  },

  async executeAction(action: SyncAction): Promise<boolean> {
    const { type, payload } = action;

    switch (type) {
        case 'CHAPTER_READ':
          return this.handleChapterRead(action);
        case 'LIBRARY_UPDATE':
          return this.handleLibraryUpdate(payload);
          case 'LIBRARY_DELETE':
            return this.handleLibraryDelete(payload);
          case 'LIBRARY_ADD':
            return this.handleLibraryAdd(payload);
          case 'SETTING_UPDATE':
          return this.handleSettingUpdate(payload);
      default:
        return true; // Unknown actions are considered "processed"
    }
  },

  async handleChapterRead(action: SyncAction) {
    const { payload, timestamp, deviceId } = action;
    const response = await fetch(`/api/library/${payload.entryId}/progress`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chapterNumber: payload.chapterNumber,
        sourceId: payload.sourceId,
        timestamp: new Date(timestamp).toISOString(),
        deviceId: deviceId,
      }),
    });

    if (!response.ok && response.status !== 409) {
      try {
        const errorData = await response.json();
        console.error(`Sync error (CHAPTER_READ) [${response.status}]:`, errorData.message || errorData.error || 'Unknown error');
      } catch {
        console.error(`Sync error (CHAPTER_READ) [${response.status}]: Failed to parse error response`);
      }
    }

    return response.ok || response.status === 409;
  },

  async handleLibraryUpdate(payload: { entryId: string; status?: string; rating?: number }) {
    const response = await fetch(`/api/library/${payload.entryId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        status: payload.status,
        rating: payload.rating,
      }),
    });

    if (!response.ok) {
      try {
        const errorData = await response.json();
        console.error(`Sync error (LIBRARY_UPDATE) [${response.status}]:`, errorData.message || errorData.error || 'Unknown error');
      } catch {
        console.error(`Sync error (LIBRARY_UPDATE) [${response.status}]: Failed to parse error response`);
      }
    }

    return response.ok;
  },

  async handleLibraryDelete(payload: { entryId: string }) {
    const response = await fetch(`/api/library/${payload.entryId}`, {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
    });
    return response.ok || response.status === 404;
  },

  async handleLibraryAdd(payload: { seriesId: string; status?: string }) {
    const response = await fetch(`/api/library`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        seriesId: payload.seriesId,
        status: payload.status || 'reading',
      }),
    });

    if (!response.ok) {
      try {
        const errorData = await response.json();
        console.error(`Sync error (LIBRARY_ADD) [${response.status}]:`, errorData.message || errorData.error || 'Unknown error');
      } catch {
        console.error(`Sync error (LIBRARY_ADD) [${response.status}]: Failed to parse error response`);
      }
    }

    return response.ok;
  },

  async handleSettingUpdate(payload: { userId: string; settings: any }) {
    // Assuming there's a user settings endpoint
    const response = await fetch(`/api/users/me`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload.settings),
    });
    return response.ok;
  },

  /**
   * Reconciles derived counters to prevent drift (BUG 85)
   * Recalculates stats from source tables rather than relying on incremental updates.
   * 
   * BUG-06 FIX: chapters_read should count actual chapter read records,
   * NOT sum of last_read_chapter values (which are chapter numbers, not counts)
   */
  async reconcileUserStats(userId: string) {
    const { prisma } = await import("@/lib/prisma");

    // BUG-06 FIX: Count chapter read records instead of summing chapter numbers
    const [chaptersReadCount, libraryCount] = await Promise.all([
      // Count actual chapter reads from the UserChapterReadV2 table
      prisma.userChapterReadV2.count({
        where: { 
          user_id: userId,
          is_read: true,
        }
      }),
      prisma.libraryEntry.count({
        where: { 
          user_id: userId,
          deleted_at: null, // Only count non-deleted entries
        }
      })
    ]);

    await prisma.user.update({
      where: { id: userId },
      data: {
        chapters_read: chaptersReadCount,
        // Add other derived fields as needed
      }
    });
  }
};
