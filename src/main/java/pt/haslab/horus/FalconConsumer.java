package pt.haslab.horus;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.haslab.horus.events.flatbuffers.generated.FalconEvent;
import pt.haslab.horus.graph.ExecutionGraphFactory;
import pt.haslab.horus.processing.EventProcessorFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;

public class FalconConsumer
        implements Runnable {
    final static Logger logger = LogManager.getLogger(FalconConsumer.class);

    private final KafkaConsumer<byte[], byte[]> consumer;

    private final FalconEventHandler handler;

    public FalconConsumer(String kafkaServers, Collection<String> topics) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServers);
        props.put("group.id", "horus");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        this.consumer = new KafkaConsumer<byte[], byte[]>(props);
        this.consumer.subscribe(topics);
        this.handler = new FalconEventHandler(EventProcessorFactory.getEventProcessor());
    }

    public void run() {
        while (true) {
            ConsumerRecords<byte[], byte[]> records = this.consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<byte[], byte[]> record : records) {
                ByteBuffer buffer = ByteBuffer.wrap(record.value());
                FalconEvent event = FalconEvent.getRootAsFalconEvent(buffer);
                this.handler.handle(event);

                if (logger.isDebugEnabled()) {
                    logger.debug("Reading event [" + event.type() + "] from [" + record.topic() + ", "
                            + record.partition() + ", " + record.offset() + "]");
                }
            }
        }
    }
}
