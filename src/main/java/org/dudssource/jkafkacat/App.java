package org.dudssource.jkafkacat;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.dudssource.jkafkacat.AppArgs.Mode;
import org.dudssource.jkafkacat.AppArgs.Offset;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * JKafkacat.
 *
 */
public class App {

	private final static Logger LOG = Logger.getLogger(App.class);

	private static AtomicBoolean SHUTDOWN = new AtomicBoolean(false);

	public static void main(String[] args) {

		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				SHUTDOWN.set(true);
			}
		});

		AppArgs appArgs = new AppArgs(args);

		if (appArgs.mode().equals(Mode.consumer)) {
			consume(appArgs);
		}

		if (appArgs.mode().equals(Mode.producer)) {
			produce(appArgs);
		}
	}

	/**
	 * Produce messages to the target broker.
	 *
	 * @param args
	 */
	private static void produce(AppArgs args) {

		Scanner scan = new Scanner(System.in);

		Properties props = args.kafkaProperties();
		props.put("auto.register.schemas", "false");

		Producer<Object, Object> producer = new KafkaProducer<>(props);

		CachedSchemaRegistryClient client = null;

		if (props.getProperty("schema.registry.url") != null) {
			client = new CachedSchemaRegistryClient(props.getProperty("schema.registry.url"), 20);
		}

		try {

			while (scan.hasNext() && !SHUTDOWN.get()) {

				String message = scan.nextLine();
				Object key = null;
				Object value = null;

				if (args.key() != null && message.indexOf(args.key()) >= 0) {

					if (message.indexOf(args.key()) > 0) {
						key = message.substring(0, message.indexOf(args.key()));
					}

					// we start with 2 because we want to ignore the start index (0) and we shift
					// one because the key character itself
					if ((message.indexOf(args.key())+2) < message.length()) {
						value = message.substring(message.indexOf(args.key())+1);
					}

				} else {
					value = message;
				}

				if (props.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
						.equals(KafkaAvroSerializer.class.getName()) && value != null) {

					try {
						SchemaMetadata metadata = client.getLatestSchemaMetadata(args.topic() + "-value");
	
						// avro schema.
					    Schema schema = new Schema.Parser().parse(metadata.getSchema());
	
					    DecoderFactory decoderFactory = new DecoderFactory();
					    DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
						Decoder decoder = decoderFactory.jsonDecoder(schema, String.valueOf(value));
					    GenericRecord record = reader.read(null, decoder);
					    value = record;
					} catch (Exception e) {
						LOG.error("Error trying to serialize Kafka Avro Value", e);
						throw e;
					}
				}

				if (props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
						.equals(KafkaAvroSerializer.class.getName()) && key != null) {

					try {

						SchemaMetadata metadata = client.getLatestSchemaMetadata(args.topic() + "-key");

						// avro schema.
					    Schema schema = new Schema.Parser().parse(metadata.getSchema());

					    DecoderFactory decoderFactory = new DecoderFactory();
					    DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
						Decoder decoder = decoderFactory.jsonDecoder(schema, String.valueOf(key));
					    GenericRecord record = reader.read(null, decoder);
					    key = record;

					} catch (Exception e) {
						LOG.error("Error trying to serialize Kafka Avro Key", e);
						throw e;
					}
				}

				ProducerRecord<Object, Object> rec = new ProducerRecord<>(args.topic(), args.partition(), key, value);
				producer.send(rec);
			}

		} catch (Exception e) {
			LOG.error("Error trying to produce messages", e);

		} finally {
			scan.close();
			producer.close();
		}
	}

	/**
	 * Consume messages from the source broker.
	 *
	 * @param groupId
	 * @return
	 */
	private static void consume(AppArgs args) {

		Properties props = args.kafkaProperties();

		// we don't allow the user to specify this
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		final Consumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(props);

		try {

			consumer.subscribe(Arrays.asList(args.topic()), new ConsumerRebalanceListener() {

				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

					for (TopicPartition topicPartition : partitions) {
						if (args.offset().equals(Offset.timestamp)) {
							Map<TopicPartition, Long> query = new HashMap<TopicPartition, Long>();
							query.put(topicPartition, args.timestamp());
							Map<TopicPartition, OffsetAndTimestamp> offsetForTimes = consumer.offsetsForTimes(query);
							if (offsetForTimes != null && offsetForTimes.get(topicPartition) != null) {
								consumer.seek(topicPartition,
										offsetForTimes.get(topicPartition).offset() + args.offsetShift());
							}
						} else if (args.offset().equals(Offset.begin)) {
							consumer.seekToBeginning(Collections.singleton(topicPartition));
							if (args.offsetShift() > 0L) {
								consumer.seek(topicPartition, consumer.position(topicPartition) + args.offsetShift());
							}
						} else if (args.offset().equals(Offset.end)) {
							consumer.seekToEnd(Collections.singleton(topicPartition));
							long current = consumer.position(topicPartition);
							if (args.offsetShift() < 0L && (current + args.offsetShift() > 0)) {
								consumer.seek(topicPartition, current + args.offsetShift());
							} else {
								consumer.seek(topicPartition, current);
							}
						}
					}
				}
			});

			int currentMessages = 0;
			boolean shouldContinue = true;

			while (shouldContinue) {

				ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofSeconds(10));

				if (records.isEmpty() && args.exit() && args.numberOfMessages() == 0) {
					shouldContinue = false;
					break;
				}

				for (ConsumerRecord<Object, Object> record : records) {

					if (args.partition() != null && !new Integer(record.partition()).equals(args.partition())) {
						continue;
					}

					// custom format
					String format = args.format();

					if (record.value() != null) {

						Object value = record.value();

						if (value instanceof GenericRecord) {
							GenericRecord avroRecord = ((GenericRecord)value);
							GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroRecord.getSchema());
							try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
								JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(avroRecord.getSchema(), baos);
								writer.write(avroRecord, jsonEncoder);
								jsonEncoder.flush();
								value = baos.toString("UTF-8");
							}
						}

						if (args.filter() != null && !String.valueOf(value).contains(args.filter())) {
							continue;
						}

						format = format.replaceFirst("\\%v", String.valueOf(value)).replaceFirst("\\%V",
								String.valueOf(value.toString().length()));
					} else {
						format = format.replaceFirst("\\%v", "NULL").replaceFirst("\\%V", "0");
					}

					if (record.key() != null) {
						format = format.replaceFirst("\\%k", record.key().toString()).replaceFirst("\\%K",
								String.valueOf(record.key().toString().length()));
					} else {
						format = format.replaceFirst("\\%k", "NULL").replaceFirst("\\%K", "0");
					}

					format = format.replaceFirst("\\%p", String.valueOf(record.partition()))
							.replaceFirst("\\%o", String.valueOf(record.offset()))
							.replaceFirst("\\%t", String.valueOf(record.topic()))
							.replaceFirst("\\%T", String.valueOf(record.timestamp())).replaceAll("\\\\n", "\n");

					System.out.println(format);

					if (SHUTDOWN.get()
							|| (args.numberOfMessages() != 0 && (++currentMessages == args.numberOfMessages()))) {
						shouldContinue = false;
						break;
					}
				}
			}

		} catch (Exception e) {
			LOG.error("Error trying to consume messages", e);

		} finally {
			consumer.close();
		}
	}
}
