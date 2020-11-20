package com.github.igalshilman.statefun.verifier;

import com.github.igalshilman.statefun.verifier.generated.SourceCommand;
import com.github.igalshilman.statefun.verifier.generated.VerificationResult;
import com.google.protobuf.Any;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

public class Constants {

  public static final IngressIdentifier<SourceCommand> IN =
      new IngressIdentifier<>(SourceCommand.class, "", "source");

  public static final EgressIdentifier<Any> OUT = new EgressIdentifier<>("", "sink", Any.class);

  public static final FunctionType FN_TYPE = new FunctionType("v", "f1");

  public static final EgressIdentifier<VerificationResult> VERIFICATION_RESULT =
      new EgressIdentifier<>("", "verification", VerificationResult.class);
}
