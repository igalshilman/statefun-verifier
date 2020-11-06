/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.igalshilman.statefun.verifier;

import org.apache.flink.statefun.flink.io.datastream.SinkFunctionSpec;
import org.apache.flink.statefun.flink.io.datastream.SourceFunctionSpec;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.github.igalshilman.statefun.verifier.Constants.IN;
import static com.github.igalshilman.statefun.verifier.Constants.OUT;

public final class Module implements StatefulFunctionModule {
  public static final Logger LOG = LoggerFactory.getLogger(Module.class);

  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    ModuleParameters moduleParameters = ModuleParameters.from(globalConfiguration);
    LOG.info(moduleParameters.toString());

    Ids ids = new Ids(moduleParameters.getNumberOfFunctionInstances());

    binder.bindIngress(new SourceFunctionSpec<>(IN, new CommandFlinkSource(moduleParameters)));
    binder.bindEgress(new SinkFunctionSpec<>(OUT, new DiscardingSink<>()));
    binder.bindIngressRouter(IN, new CommandRouter(ids));

    FunctionProvider provider = new FunctionProvider(ids, asyncExecutorService());
    binder.bindFunctionProvider(Constants.FN_TYPE, provider);
  }

  private static ScheduledExecutorService asyncExecutorService() {
    return Executors.newScheduledThreadPool(
        0,
        r -> {
          Thread t = new Thread(r);
          t.setDaemon(true);
          t.setName("command-interpreter-executor-service");
          return t;
        });
  }
}
