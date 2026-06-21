package io.github.yoonryeol.bitcoinrealtime.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpbitTradeDeserializationSchema implements KafkaRecordDeserializationSchema<UpbitTrade> {
    private static final Logger LOG = LoggerFactory.getLogger(UpbitTradeDeserializationSchema.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<UpbitTrade> out) {
        String jsonString = new String(record.value(), java.nio.charset.StandardCharsets.UTF_8);
        try {
            JsonNode node = MAPPER.readTree(jsonString);
            UpbitTrade trade = new UpbitTrade();
            trade.code = node.get("code").asText();
            trade.tradePrice = node.get("trade_price").asDouble();
            trade.tradeVolume = node.get("trade_volume").asDouble();
            trade.tradeTimestamp = node.get("trade_timestamp").asLong();
            trade.askBid = node.get("ask_bid").asText();
            trade.sequentialId = node.get("sequential_id").asLong();
            out.collect(trade);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize record: {}", e.getMessage());
        }
    }

    @Override
    public TypeInformation<UpbitTrade> getProducedType() {
        return TypeInformation.of(UpbitTrade.class);
    }
}
