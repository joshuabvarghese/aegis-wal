package com.aegis.tiering;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

import com.aegis.tiering.RemoteCatalog.CatalogEntry;

/**
 * In-memory catalog mapping baseOffset → remote MinIO object key.
 *
 * When a read request arrives for a logical offset that exceeds the local
 * segment boundary, the HotStorageEngine consults this catalog to route
 * the fetch to the cold tier transparently.
 *
 * Structure: baseOffset -> CatalogEntry(objectKey, baseOffset, recordCount, sizeBytes)
 */
public class RemoteCatalog {

    public record CatalogEntry(
        String objectKey,
        long   baseOffset,
        long   endOffset,
        long   sizeBytes,
        String bucketUrl
    ) {}

    private static final RemoteCatalog INSTANCE = new RemoteCatalog();
    private final NavigableMap<Long, CatalogEntry> entries = new ConcurrentSkipListMap<>();
    private long totalUploadedBytes = 0L;
    private int  totalSegmentsUploaded = 0;

    private RemoteCatalog() {}
    public static RemoteCatalog get() { return INSTANCE; }

    public void register(CatalogEntry entry) {
        entries.put(entry.baseOffset(), entry);
        totalUploadedBytes += entry.sizeBytes();
        totalSegmentsUploaded++;
    }

    /**
     * Find the remote catalog entry for a given logical offset.
     * Returns the entry whose key range contains the requested offset.
     */
    public Optional<CatalogEntry> findForOffset(long logicalOffset) {
        var floorEntry = entries.floorEntry(logicalOffset);
        if (floorEntry == null) return Optional.empty();

        CatalogEntry candidate = floorEntry.getValue();
        if (logicalOffset <= candidate.endOffset()) {
            return Optional.of(candidate);
        }
        return Optional.empty();
    }

    public long totalUploadedBytes()    { return totalUploadedBytes; }
    public int  totalSegmentsUploaded() { return totalSegmentsUploaded; }
    public int  size()                  { return entries.size(); }

    public void clear() {
        entries.clear();
        totalUploadedBytes = 0;
        totalSegmentsUploaded = 0;
    }
}
