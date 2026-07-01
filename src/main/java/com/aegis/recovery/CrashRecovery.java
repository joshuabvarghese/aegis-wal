package com.aegis.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import com.aegis.core.AegisConfig;
import com.aegis.core.EventBus;
import com.aegis.core.WalRecord;
import com.aegis.recovery.CrashRecovery.RecoveryResult;

/**
 * Module C: Crash Recovery
 *
 * On startup, scans all .log files in the node data directory.
 * For each file:
 * - Reads frames sequentially.
 * - Validates CRC32C checksums.
 * - Detects torn writes (partially written final frames).
 * - Truncates the file to the last fully committed record boundary.
 *
 * Returns the recovered max offset so the storage engine can resume from there.
 */
public class CrashRecovery {

    private final int nodeId;
    private final Path dataDir;

    public record RecoveryResult(
        long recoveredMaxOffset,
        int  segmentsScanned,
        int  recordsRecovered,
        int  tornWritesTruncated,
        List<String> report
    ) {}

    public CrashRecovery(int nodeId) {
        this.nodeId = nodeId;
        this.dataDir = AegisConfig.dataDir(nodeId);
    }

    public RecoveryResult recover() throws IOException {
        List<String> report = new ArrayList<>();
        
        File dataDirFile = dataDir.toFile();
        if (!dataDirFile.exists()) {
            dataDirFile.mkdirs();
        }

        // Fetch files using our helper and pipe them into a mutable ArrayList
        List<Path> logFiles = new ArrayList<>(findSegmentFiles());
        // Sort them by file name sequentially
        logFiles.sort(Comparator.comparing(p -> p.getFileName().toString()));

        long maxOffset = 0;
        int segmentsScanned = 0;
        int recordsRecovered = 0;
        int tornWrites = 0;

        EventBus.get().publish(EventBus.EventType.RECOVERY_SCAN, nodeId,
            "Recovery scan: found " + logFiles.size() + " segment(s)");

        for (Path logFile : logFiles) {
            segmentsScanned++;
            long baseOffset = parseBaseOffset(logFile);

            report.add("[SCAN] segment=" + logFile.getFileName() + " base_offset=" + baseOffset);
            EventBus.get().publish(EventBus.EventType.RECOVERY_SCAN, nodeId,
                "Scanning " + logFile.getFileName());

            long lastGoodPosition = 0;
            long lastGoodOffset = baseOffset;
            int recordsInSegment = 0;
            boolean foundTear = false;

            try (FileChannel channel = FileChannel.open(logFile, StandardOpenOption.READ)) {
                long size = channel.size();
                if (size == 0) {
                    report.add("  → Empty segment, skipping");
                    continue;
                }

                channel.position(0);

                while (channel.position() < size) {
                    long frameStart = channel.position();

                    // Try to read the fixed header first
                    ByteBuffer headerBuf = ByteBuffer.allocate(WalRecord.HEADER_SIZE);
                    int headerRead = channel.read(headerBuf);
                    if (headerRead < WalRecord.HEADER_SIZE) {
                        // Torn write: partial header at end of file
                        foundTear = true;
                        report.add("  → TORN WRITE detected at position " + frameStart + " (partial header)");
                        break;
                    }
                    headerBuf.flip();

                    int payloadLen = headerBuf.getInt();
                    long offset    = headerBuf.getLong();
                    long timestamp = headerBuf.getLong();
                    int  storedCrc = headerBuf.getInt();

                    if (payloadLen < 0 || payloadLen > 64 * 1024 * 1024) {
                        foundTear = true;
                        report.add("  → CORRUPT length prefix (" + payloadLen + ") at pos=" + frameStart);
                        break;
                    }

                    // Check if payload is fully present
                    if (channel.position() + payloadLen > size) {
                        foundTear = true;
                        report.add("  → TORN WRITE at offset=" + offset + ": payload truncated");
                        break;
                    }

                    ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
                    channel.read(payloadBuf);
                    payloadBuf.flip();
                    byte[] payload = new byte[payloadLen];
                    payloadBuf.get(payload);

                    // Verify CRC
                    int computedCrc = WalRecord.computeCrc(offset, timestamp, payload);
                    if (computedCrc != storedCrc) {
                        foundTear = true;
                        report.add("  → CRC MISMATCH at offset=" + offset
                            + " stored=0x" + Integer.toHexString(storedCrc)
                            + " computed=0x" + Integer.toHexString(computedCrc));
                        break;
                    }

                    // Record is valid
                    lastGoodPosition = channel.position();
                    lastGoodOffset = offset;
                    recordsInSegment++;
                    recordsRecovered++;
                    maxOffset = Math.max(maxOffset, offset);
                }
            }

            report.add("  → " + recordsInSegment + " valid records, last_offset=" + lastGoodOffset);

            if (foundTear) {
                tornWrites++;
                report.add("  → TRUNCATING to position " + lastGoodPosition);
                try (FileChannel writeChannel = FileChannel.open(logFile, StandardOpenOption.WRITE)) {
                    writeChannel.truncate(lastGoodPosition);
                }
                EventBus.get().publish(EventBus.EventType.RECOVERY_SCAN, nodeId,
                    "TRUNCATED " + logFile.getFileName() + " to " + lastGoodPosition);
            }
        }

        report.add("Recovery complete: max_offset=" + maxOffset
            + " segments=" + segmentsScanned + " records=" + recordsRecovered
            + " torn_writes=" + tornWrites);

        EventBus.get().publish(EventBus.EventType.RECOVERY_SCAN, nodeId,
            "Recovery OK max_offset=" + maxOffset + " torn=" + tornWrites);

        return new RecoveryResult(maxOffset, segmentsScanned, recordsRecovered, tornWrites, report);
    }

    private List<Path> findSegmentFiles() throws IOException {
        if (!dataDir.toFile().exists()) return List.of();
        try (Stream<Path> stream = Files.walk(dataDir, 1)) {
            return stream
                .filter(p -> p.toString().endsWith(".log"))
                .toList();
        }
    }

    private long parseBaseOffset(Path logFile) {
        String name = logFile.getFileName().toString().replace(".log", "");
        try { return Long.parseLong(name); } catch (NumberFormatException e) { return 0; }
    }
}