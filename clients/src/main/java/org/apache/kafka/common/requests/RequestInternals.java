// (Copyright) [2017 - 2017] Confluent, Inc.

package org.apache.kafka.common.requests;

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.types.Struct;

// Expose some stuff from org.apache.kafka.common.requests that wasn't meant to be exposed
// Used by ce-broker-plugins
public class RequestInternals {

  public static Struct toStruct(AbstractRequest request) {
    return request.toStruct();
  }

  public static Struct toStruct(AbstractResponse response, short version) {
    return response.toStruct(version);
  }

  public static Send toSend(AbstractResponse response, short apiVersion, String destination,
                            ResponseHeader header) {
    return response.toSend(destination, header, apiVersion);
  }

}
