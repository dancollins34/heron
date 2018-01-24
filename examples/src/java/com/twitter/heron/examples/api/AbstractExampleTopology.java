//  Copyright 2018 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.examples.api;


import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.simulator.Simulator;

public interface AbstractExampleTopology {
  HeronTopology buildTopology();
  Config buildConfig();

  default void run(String[] args) throws Exception{
    String topologyName = "test";
    boolean shouldSimulate = false;
    long simulatorTimeoutMillis = 10000;

    switch (args.length) {
      case 0:
        System.out.println("No topology name provided, using simulator locally.");
        shouldSimulate = true;
        break;
      case 1:
        topologyName = args[0];
        break;
      case 2:
        topologyName = args[0];
        shouldSimulate = args[1].equals("1");
        break;
      case 3:
        topologyName = args[0];
        shouldSimulate = args[1].equals("1");
        simulatorTimeoutMillis = Long.parseLong(args[2]);
        break;
       default:
         throw new RuntimeException("Improper arguments to example topology. Example topologies " +
             "accept up to 3 arguments, the topology name, 1/0 whether to simulate the topology," +
             "and the timeout of the simulator in milliseconds.");
    }

    if(shouldSimulate){
      System.out.println("Topology name not provided as an argument, running in simulator mode.");
      Simulator simulator = new Simulator();
      simulator.submitTopology(topologyName, this.buildConfig(), this.buildTopology());
      Utils.sleep(10000);
      simulator.killTopology(topologyName);
      simulator.shutdown();
    } else {
      HeronSubmitter.submitTopology(topologyName, this.buildConfig(), this.buildTopology());
    }
  }
}
