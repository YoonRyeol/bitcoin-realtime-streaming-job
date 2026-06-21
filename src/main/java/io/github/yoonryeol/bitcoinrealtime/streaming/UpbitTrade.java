package io.github.yoonryeol.bitcoinrealtime.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class UpbitTrade {
    public String type;
    public String code;
    public double tradePrice;
    public double tradeVolume;
    public long tradeTimestamp;
    public String askBid;
    @JsonProperty("sequential_id")
    public long sequentialId;
    public String streamType;

    public UpbitTrade() {}

    // Getters and setters for POJO
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getCode() { return code; }
    public void setCode(String code) { this.code = code; }

    public double getTradePrice() { return tradePrice; }
    public void setTradePrice(double tradePrice) { this.tradePrice = tradePrice; }

    public double getTradeVolume() { return tradeVolume; }
    public void setTradeVolume(double tradeVolume) { this.tradeVolume = tradeVolume; }

    public long getTradeTimestamp() { return tradeTimestamp; }
    public void setTradeTimestamp(long tradeTimestamp) { this.tradeTimestamp = tradeTimestamp; }

    public String getAskBid() { return askBid; }
    public void setAskBid(String askBid) { this.askBid = askBid; }

    public long getSequentialId() { return sequentialId; }
    public void setSequentialId(long sequentialId) { this.sequentialId = sequentialId; }

    public String getStreamType() { return streamType; }
    public void setStreamType(String streamType) { this.streamType = streamType; }
}
