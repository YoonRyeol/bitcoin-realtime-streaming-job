package io.github.yoonryeol.bitcoinrealtime.streaming;

public class RankingEntry {
    public int rank;
    public String code;
    public double sumTradeVolume;
    public double sumTradeAmount;
    public double metric;
    public long tradeCount;
    public long windowEnd;

    public RankingEntry() {}

    public RankingEntry(int rank, String code, double sumTradeVolume, double sumTradeAmount, double metric, long tradeCount, long windowEnd) {
        this.rank = rank;
        this.code = code;
        this.sumTradeVolume = sumTradeVolume;
        this.sumTradeAmount = sumTradeAmount;
        this.metric = metric;
        this.tradeCount = tradeCount;
        this.windowEnd = windowEnd;
    }

    // Getters and setters
    public int getRank() { return rank; }
    public void setRank(int rank) { this.rank = rank; }

    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }

    public double getSumTradeVolume() { return sumTradeVolume; }
    public void setSumTradeVolume(double sumTradeVolume) { this.sumTradeVolume = sumTradeVolume; }

    public double getSumTradeAmount() { return sumTradeAmount; }
    public void setSumTradeAmount(double sumTradeAmount) { this.sumTradeAmount = sumTradeAmount; }

    public double getMetric() { return metric; }
    public void setMetric(double metric) { this.metric = metric; }

    public long getTradeCount() { return tradeCount; }
    public void setTradeCount(long tradeCount) { this.tradeCount = tradeCount; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }
}
