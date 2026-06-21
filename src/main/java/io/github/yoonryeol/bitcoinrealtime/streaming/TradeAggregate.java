package io.github.yoonryeol.bitcoinrealtime.streaming;

public class TradeAggregate {
    public String code;
    public double sumTradeVolume;
    public double sumTradeAmount;
    public long tradeCount;

    public TradeAggregate() {}

    public TradeAggregate(String code) {
        this.code = code;
        this.sumTradeVolume = 0;
        this.sumTradeAmount = 0;
        this.tradeCount = 0;
    }

    // Getters and setters
    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }

    public double getSumTradeVolume() { return sumTradeVolume; }
    public void setSumTradeVolume(double sumTradeVolume) { this.sumTradeVolume = sumTradeVolume; }

    public double getSumTradeAmount() { return sumTradeAmount; }
    public void setSumTradeAmount(double sumTradeAmount) { this.sumTradeAmount = sumTradeAmount; }

    public long getTradeCount() { return tradeCount; }
    public void setTradeCount(long tradeCount) { this.tradeCount = tradeCount; }
}
