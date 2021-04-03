package pt.haslab.horus;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URL;

public class Config {
    final static Logger logger = LogManager.getLogger(FalconConsumer.class);

    private static Config instance;

    private static Configuration config;

    private Config(Configuration config) {
        Config.config = config;
    }

    public static Config getInstance() {
        if (null == instance) {
            try {
                URL configUrl = Config.class.getClassLoader().getResource("config.properties");
                Configuration config = new PropertiesConfiguration(configUrl);

                logger.info("Loading configuration file " + configUrl);

                instance = new Config(config);
            } catch (ConfigurationException ex) {
                throw new RuntimeException(ex);
            }
        }
        return instance;
    }

    public Configuration getConfig() {
        return config;
    }
}
