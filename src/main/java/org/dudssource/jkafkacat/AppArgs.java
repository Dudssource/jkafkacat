package org.dudssource.jkafkacat;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;

/**
 * Application arguments.
 *
 * @author eduardo.goncalves
 *
 */
public class AppArgs {

	private final static Logger LOG = Logger.getLogger(AppArgs.class);

	@Option(name = "-m", aliases = { "--mode" }, usage = "Kafka mode", required = true)
	private Mode mode;

	@Option(name = "-e", aliases = { "--exit" }, usage = "Exit when there're no more messages to process", required = false)
	private Boolean exit = false;

	@Option(name = "-f", aliases = { "--format" }, usage = "Print format: [ %v : value, %V : value length in bytes, %k : key, %K : key length in bytes, %p : partition, %t : topic, %T : record timestamp, %o : record offset ]", required = false)
	private String format = "%v";

	@Option(name = "-t", aliases = { "--topic" }, usage = "Kafka target topic", required = true)
	private String topic;

	@Option(name = "-p", aliases = { "--partition" }, usage = "Partition to consume/produce", required = false)
	private Integer partition;

	@Option(name = "-c", aliases = { "--config" }, usage = "Kafka configuration file", required = true)
	private String config;

	@Option(name = "-ks", aliases = { "--keyStore" }, usage = "Java Keystore", required = false)
	private String keystore;

	@Option(name = "-ksp", aliases = {
			"--keyStorePass" }, usage = "Java Keystore pass", required = false, depends = "-ks")
	private String keystorePass;

	@Option(name = "-ts", aliases = { "--trustStore" }, usage = "Java Truststore", required = false)
	private String truststore;

	@Option(name = "-tsp", aliases = {
			"--trustStorePass" }, usage = "Java Truststore pass", required = false, depends = "-ts")
	private String truststorePass;

	@Option(name = "-o", aliases = {
			"--offset" }, usage = "Consumer current offset mode", required = false, depends = "-m")
	private Offset offset = Offset.none;

	@Option(name = "-os", aliases = {
	"--offsetShift" }, usage = "Offset shift (move forward or backward)", required = false, depends = "-o")
	private long offsetShift = 0L;

	@Option(name = "-nm", aliases = { "--numberOfMessages" }, usage = "Number of messages to consume", required = false)
	private Integer numberOfMessages = 0;

	@Option(name = "-ot", aliases = {
			"--offsetTimestamp" }, usage = "Timestamp (Millis UTC) to search for offsets, required when offset = 'timestamp'", required = false, depends = "-o")
	private Long timestamp;

	@Option(name = "-K", aliases = {
	"--key" }, usage = "The character used to split the key", required = false)
	private String key;

	@Option(name = "-F", aliases = {
			"--filter" }, usage = "The filter used to select consumed messages", required = false, depends = "-m")
	private String filter;

	private Properties kafkaProperties;

	public AppArgs(String... args) {

		final CmdLineParser parser = new CmdLineParser(this);

		try {

			// parse the arguments.
			parser.parseArgument(args);

			if (offset.equals(Offset.timestamp) && timestamp == null) {
				throw new IllegalArgumentException("Parameter timestamp is required when offset = 'timestamp'");
			}

			// load properties
			kafkaProperties = loadProps(config());

			if (keystore != null) {
				System.setProperty("javax.net.ssl.keyStore", keystore);
				System.setProperty("javax.net.ssl.keyStorePassword", keystorePass);
			}

			if (truststore != null) {
				System.setProperty("javax.net.ssl.trustStore", truststore);
				System.setProperty("javax.net.ssl.trustStorePassword", truststorePass);
			}

		} catch (Exception e) {

			LOG.error("Error trying to extract program options: " + e.getMessage());
			LOG.error("java program.jar [options...]");
			parser.printUsage(System.err);
			LOG.error("\n  Example: java program.jar " + parser.printExample(OptionHandlerFilter.ALL));

			// saindo do programa com erro
			System.exit(1);
		}
	}

	public static enum Mode {
		consumer, producer
	}

	public static enum Offset {
		begin, end, none, timestamp
	}

	/**
	 * Load the configuration file.
	 *
	 * @param path
	 * @return
	 */
	private static Properties loadProps(String path) {

		LOG.debug(String.format("Loading kafka consumer properties from %s", path));

		Properties props = new Properties();

		try (FileInputStream is = new FileInputStream(path)) {

			props.load(is);

		} catch (Exception e) {

			LOG.error("Error trying to load the properties file", e);

			throw new RuntimeException(e);
		}

		return props;
	}

	/**
	 * Returns {@link #config}
	 * 
	 * @return
	 */
	public String config() {
		return config;
	}

	public Properties kafkaProperties() {
		return kafkaProperties;
	}

	public Mode mode() {
		return mode;
	}

	public String topic() {
		return topic;
	}

	public String format() {
		return format;
	}

	public Offset offset() {
		return offset;
	}

	public Long timestamp() {
		return timestamp;
	}

	public Integer numberOfMessages() {
		return numberOfMessages;
	}

	public long offsetShift() {
		return offsetShift;
	}

	public Integer partition() {
		return partition;
	}

	public Boolean exit() {
		return exit;
	}

	public String key() {
		return key;
	}

	public String filter() {
		return filter;
	}
}