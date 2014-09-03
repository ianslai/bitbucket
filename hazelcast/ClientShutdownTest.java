import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;
import com.hazelcast.logging.LoggerFactory;
import com.hazelcast.logging.LoggerFactorySupport;
import com.hazelcast.logging.StandardLoggerFactory;

public class ClientShutdownTest {

    // Set up custom logger here since HazelcastClient statically obtains a Logger
    // and thus makes it impossible to configure normally at runtime
    {
        System.setProperty("hazelcast.logging.class", CapturingLoggerFactory.class.getName());
        try {
            Class.forName("com.hazelcast.client.HazelcastClient");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private List<String> addresses = Arrays.asList("localhost:5800", "localhost:5801");
    private HazelcastInstance server1, server2, client;
    private static CapturingLoggerFactory loggerFactory;

    @Before
    public void setUp() {
        server1 = createServer(5800);
        server2 = createServer(5801);
        client = createClient();
        loggerFactory.reset();
    }

    @After
    public void tearDown() {
        client.shutdown();
        server1.shutdown();
        server2.shutdown();
    }

    @Test
    public void testClientShutdownDuringReconnectCausesIllegalStateException() throws Exception {
        server1.shutdown();
        server2.shutdown();
        Thread.sleep(3000);
        client.shutdown();
        Thread.sleep(1000);
        assertFalse(String.valueOf(loggerFactory.severeThrowable), loggerFactory.severeThrown);
    }

    @Test
    public void testClientShutdownDuringReconnectCausesSevereLoggingFromClientPartitionService() throws Exception {
        server1.shutdown();
        server2.shutdown();
        client.shutdown();
        Thread.sleep(1000);
        assertFalse(String.valueOf(loggerFactory.severeThrowable), loggerFactory.severeThrown);
    }

    private HazelcastInstance createClient() {
        System.setProperty("hazelcast.logging.class", CapturingLoggerFactory.class.getName());

        ClientConfig clientConfig = new ClientConfig();

        GroupConfig group = clientConfig.getGroupConfig();
        group.setName("group");
        group.setPassword("password");

        ClientNetworkConfig networkConfig = new ClientNetworkConfig();
        networkConfig.setAddresses(addresses);
        networkConfig.setRedoOperation(true);
        networkConfig.setConnectionAttemptLimit(Integer.MAX_VALUE);
        networkConfig.setConnectionAttemptPeriod(5000);
        clientConfig.setNetworkConfig(networkConfig);

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    private HazelcastInstance createServer(int port) {
        System.clearProperty("hazelcast.logging.class");

        Config config = new com.hazelcast.config.Config("r9." + port);
        config.setProperty(GroupProperties.PROP_SOCKET_BIND_ANY, "false");

        GroupConfig group = config.getGroupConfig();
        group.setName("group");
        group.setPassword("password");

        NetworkConfig network = config.getNetworkConfig();
        network.setPort(port);
        network.setPortAutoIncrement(false);

        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().setMembers(addresses);

        network.getInterfaces().setEnabled(false);

        HazelcastInstance hazelcastServer = Hazelcast.newHazelcastInstance(config);

        return hazelcastServer;

    }

    private static class CapturingLoggerFactory extends LoggerFactorySupport implements LoggerFactory {

        private boolean severeThrown ;
        private Throwable severeThrowable;
        private final StandardLoggerFactory logger = new StandardLoggerFactory();

        public CapturingLoggerFactory() {
            System.err.println("Factory created: " + hashCode());
            loggerFactory = this;
            reset();
        }

        public void reset() {
            severeThrown = false;
            severeThrowable = null;
        }

        @Override
        protected ILogger createLogger(String name) {
            return new CapturingLogger(name);
        }

        private class CapturingLogger extends AbstractLogger implements ILogger {

            private ILogger delegate;

            public CapturingLogger(String name) {
                delegate = logger.getLogger(name);
            }

            @Override
            public void log(Level level, String message) {
                if (Level.SEVERE.equals(level)) {
                    severeThrown = true;
                }
                delegate.log(level, message);
            }

            @Override
            public void log(Level level, String message, Throwable thrown) {
                if (Level.SEVERE.equals(level)) {
                    severeThrown = true;
                    severeThrowable = thrown;
                }
                delegate.log(level, message, thrown);
            }

            @Override
            public void log(LogEvent logEvent) {
                if (Level.SEVERE.equals(logEvent.getLogRecord().getLevel())) {
                    severeThrown = true;
                    severeThrowable = logEvent.getLogRecord().getThrown();
                }
                delegate.log(logEvent);
            }

            @Override
            public Level getLevel() {
                return delegate.getLevel();
            }

            @Override
            public boolean isLoggable(Level level) {
                return delegate.isLoggable(level);
            }
        }

    }
}
