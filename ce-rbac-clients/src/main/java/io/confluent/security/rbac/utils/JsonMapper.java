// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

public class JsonMapper {
  private static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new ObjectMapper();
    OBJECT_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    OBJECT_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    OBJECT_MAPPER.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
    OBJECT_MAPPER.registerModule(new Jdk8Module());
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addSerializer(KafkaPrincipal.class, new PrincipalSerializer(KafkaPrincipal.class));
    simpleModule.addDeserializer(KafkaPrincipal.class, new PrincipalDeserializer(KafkaPrincipal.class));
    OBJECT_MAPPER.registerModule(simpleModule);
  }

  public static ObjectMapper objectMapper() {
        return OBJECT_MAPPER;
  }

  private static class PrincipalSerializer extends StdSerializer<KafkaPrincipal> {

    public PrincipalSerializer(Class<KafkaPrincipal> t) {
      super(t);
    }

    @Override
    public void serialize(KafkaPrincipal principal, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeString(principal.toString());
    }
  }

  private static class PrincipalDeserializer extends StdDeserializer<KafkaPrincipal> {

    public PrincipalDeserializer(Class<KafkaPrincipal> t) {
      super(t);
    }

    @Override
    public KafkaPrincipal deserialize(JsonParser jsonParser,
        DeserializationContext deserializationContext) throws IOException {
      return SecurityUtils.parseKafkaPrincipal(jsonParser.getValueAsString());
    }
  }
}
