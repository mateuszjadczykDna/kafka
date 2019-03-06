package io.confluent.common;

import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public class InterClusterConnection {

    private static final String ADVERTISED_LISTENERS_CONFIG = "advertised.listeners";
    private static final String LISTENER_SECURITY_PROTOCOL = "listener.security.protocol.map";


    /**
     * @param listener Type of listener you want to connect to. Usually "INTERNAL" or "EXTERNAL"
     * @param cfgMap broker config of the broker you'll connect to. Should include
     *               advertised.listeners that includes the listener you want
     * @return a string that you can use as "bootstrap.servers" config to connect to that broker
     * @throws ConfigException
     */
    public static String getBootstrapBrokerForListener(String listener, Map<String, ?> cfgMap)
            throws ConfigException {
        String bootstrapBroker = null;
        if (cfgMap.get(ADVERTISED_LISTENERS_CONFIG) != null) {
            if (cfgMap.get(ADVERTISED_LISTENERS_CONFIG) instanceof String) {
                final int brokerStartIndex = listener.length() + 3;
                final String advertisedListeners = (String) cfgMap.get(ADVERTISED_LISTENERS_CONFIG);
                String[] listeners = advertisedListeners.split(",");
                for (String advertisedListener: listeners) {
                    if (advertisedListener.contains(listener)
                            && advertisedListener.length() > brokerStartIndex) {
                        bootstrapBroker = advertisedListener.substring(brokerStartIndex);
                        break;
                    }
                }
            }
            if (bootstrapBroker == null) {
                throw new ConfigException(String.format(
                        "Expected to find %s listener in %s config", listener, ADVERTISED_LISTENERS_CONFIG));
            }

        } else {
            throw new ConfigException(
                    String.format("%s config is required to validate topic creation",
                            ADVERTISED_LISTENERS_CONFIG));
        }

        return bootstrapBroker;
    }

    /**
     * @param listener Type of listener you want to connect to. Usually "INTERNAL" or "EXTERNAL"
     * @param cfgMap broker config of the broker you'll connect to. Should the mapping from
     *               listeners to security protocols
     * @return String with the security protocol that is supported for the listener you picked
     * @throws ConfigException
     */
    public static String getListenerSecurityProtocol(String listener, Map<String, ?> cfgMap)
            throws ConfigException {
        String protocol = null;
        if (cfgMap.get(LISTENER_SECURITY_PROTOCOL) != null
                && cfgMap.get(LISTENER_SECURITY_PROTOCOL) instanceof String) {
            final int protocolStartIndex = listener.length() + 1;
            final String listenerSecurityProtocols = (String) cfgMap.get(LISTENER_SECURITY_PROTOCOL);
            String[] securityProtocolsList = listenerSecurityProtocols.split(",");
            for (String securityProtocol: securityProtocolsList) {
                if (securityProtocol.contains(listener)
                        && securityProtocol.length() > protocolStartIndex) {
                    protocol = securityProtocol.substring(protocolStartIndex);
                    break;
                }
            }
        }
        return protocol;
    }

}
