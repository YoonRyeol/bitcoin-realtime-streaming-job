FROM flink:1.19.1-java17

# Copy the job JAR to Flink's usrlib directory
COPY build/libs/bitcoin-realtime-streaming-job-1.0-SNAPSHOT-all.jar /opt/flink/usrlib/bitcoin-realtime-streaming-job.jar

# Set the job manager entry point
ENV FLINK_PROPERTIES=""
