import { addToLibrary } from '@/lib/actions/library-actions';
import { prisma } from '@/lib/prisma';
import { createClient } from '@/lib/supabase/server';

jest.mock('@/lib/prisma', () => ({
    prisma: {
      $transaction: jest.fn(),
      $executeRaw: jest.fn().mockResolvedValue(1),
      $queryRawUnsafe: jest.fn().mockResolvedValue([]),
      series: {
        findUnique: jest.fn(),
        update: jest.fn().mockResolvedValue({}),
      },
      libraryEntry: {
        findUnique: jest.fn(),
        upsert: jest.fn(),
      },
    },
}));

jest.mock('@/lib/supabase/server', () => ({
  createClient: jest.fn(),
}));

jest.mock('next/cache', () => ({
  revalidatePath: jest.fn(),
}));

describe('Library Integration', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should add a series to the library successfully', async () => {
    const mockUser = { id: 'user-123' };
    const mockSeriesId = '00000000-0000-0000-0000-000000000001';
    
    (createClient as jest.Mock).mockResolvedValue({
      auth: {
        getUser: jest.fn().mockResolvedValue({ data: { user: mockUser } }),
      },
      from: jest.fn().mockReturnValue({
        insert: jest.fn().mockResolvedValue({ error: null }),
      }),
    });

    const mockSeries = {
      id: mockSeriesId,
      sources: [{ source_url: 'http://test.com', source_name: 'Test' }]
    };

    const mockLibraryEntry = { 
      id: 'entry-1', 
      series_id: mockSeriesId,
      last_read_chapter: 0 
    };

    (prisma.$transaction as jest.Mock).mockImplementation(async (callback) => {
      const txMock = {
        series: {
          findUnique: jest.fn().mockResolvedValue(mockSeries),
          update: jest.fn().mockResolvedValue({}),
        },
        libraryEntry: {
          findUnique: jest.fn().mockResolvedValue(null),
          upsert: jest.fn().mockResolvedValue(mockLibraryEntry),
        },
      };
      return callback(txMock);
    });

    const result = await addToLibrary(mockSeriesId);
    
    expect(result).toHaveProperty('data');
    expect(result.data.series_id).toBe(mockSeriesId);
  });

  it('should return error if not authenticated', async () => {
    (createClient as jest.Mock).mockResolvedValue({
      auth: {
        getUser: jest.fn().mockResolvedValue({ data: { user: null } }),
      },
    });

    const result = await addToLibrary('00000000-0000-0000-0000-000000000001');
    expect(result).toEqual({ error: 'Not authenticated' });
  });
});
