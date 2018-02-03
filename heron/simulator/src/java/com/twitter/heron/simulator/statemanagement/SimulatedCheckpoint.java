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
package com.twitter.heron.simulator.statemanagement;

import java.io.Serializable;

import com.twitter.heron.api.state.State;

public class SimulatedCheckpoint {
  private String checkpointId;
  private String component;
  private Integer taskId;
  private State<Serializable, Serializable> state;

  /**
   * An object like import com.twitter.heron.spi.statefulstorage.Checkpoint for the simulator which
   * does not serialize and deserialize the state, but contains enough information for the purposes
   * of com.twitter.heron.simulator.statemanagement.StateManager
   *
   * @param checkpointId the identifier of this checkpoint, increasing over time
   * @param component the name of the component this is for
   * @param taskId the id of the task for this component that this is from
   * @param state the state to be stored
   */
  public SimulatedCheckpoint(
      String checkpointId,
      String component,
      Integer taskId,
      State<Serializable, Serializable> state
  ) {
    this.checkpointId = checkpointId;
    this.component = component;
    this.taskId = taskId;
    this.state = state;
  }

  public String getCheckpointId() {
    return checkpointId;
  }

  public String getComponent() {
    return component;
  }

  public Integer getTaskId() {
    return taskId;
  }

  public State<Serializable, Serializable> getState() {
    return state;
  }
}
