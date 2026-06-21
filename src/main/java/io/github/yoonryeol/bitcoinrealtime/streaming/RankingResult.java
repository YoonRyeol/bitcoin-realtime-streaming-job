package io.github.yoonryeol.bitcoinrealtime.streaming;

import java.util.List;

public class RankingResult {
    public long windowStart;
    public long windowEnd;
    public List<RankingEntry> entries;

    public RankingResult() {}

    public RankingResult(long windowStart, long windowEnd, List<RankingEntry> entries) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.entries = entries;
    }

    // Getters and setters
    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }

    public List<RankingEntry> getEntries() { return entries; }
    public void setEntries(List<RankingEntry> entries) { this.entries = entries; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RankingResult{windowStart=").append(windowStart)
          .append(", windowEnd=").append(windowEnd)
          .append(", entries=[");
        if (entries != null) {
            for (int i = 0; i < entries.size(); i++) {
                RankingEntry entry = entries.get(i);
                if (i > 0) sb.append(", ");
                sb.append("RankingEntry{rank=").append(entry.rank)
                  .append(", code=").append(entry.code)
                  .append(", volume=").append(entry.sumTradeVolume)
                  .append(", amount=").append(entry.sumTradeAmount)
                  .append(", metric=").append(entry.metric)
                  .append(", count=").append(entry.tradeCount)
                  .append(", windowEnd=").append(entry.windowEnd)
                  .append("}");
            }
        }
        sb.append("]}");
        return sb.toString();
    }
}
