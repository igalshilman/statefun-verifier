package com.github.igalshilman.statefun.verifier.e2e;

import com.github.igalshilman.statefun.verifier.Constants;
import com.github.igalshilman.statefun.verifier.generated.VerificationResult;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.statefun.flink.io.datastream.SinkFunctionSpec;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static com.github.igalshilman.statefun.verifier.e2e.Constants.HOST_KEY;
import static com.github.igalshilman.statefun.verifier.e2e.Constants.PORT_KEY;

public final class VerifierModule implements StatefulFunctionModule {

  @Override
  public void configure(Map<String, String> map, Binder binder) {
    String host = map.get(HOST_KEY);
    Objects.requireNonNull(host, HOST_KEY + " is missing.");

    String portAsString = map.get(PORT_KEY);
    Objects.requireNonNull(portAsString, PORT_KEY + " is missing");
    final int port = Integer.parseInt(portAsString);

    SocketClientSink<VerificationResult> client =
        new SocketClientSink<>(host, port, new VerificationResultSerializer(), 3, true);

    binder.bindEgress(new SinkFunctionSpec<>(Constants.VERIFICATION_RESULT, client));
  }

  private static final class VerificationResultSerializer
      implements SerializationSchema<VerificationResult> {

    @Override
    public byte[] serialize(VerificationResult element) {
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream(element.getSerializedSize() + 8);
        element.writeDelimitedTo(out);
        return out.toByteArray();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
