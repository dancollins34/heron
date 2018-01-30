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

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.twitter.heron.proto.system.PhysicalPlans;

public class StateManager{
  /**
   * Mix of a statefulstorage and some aspects of checkpoint master functionality
   */

  // Order is as follows:
  // storage -> all states for the topology
  // storage.get(String checkpointId) = checkpointIdStorage -> one topology's states for
  //    one checkpoint
  // checkpointIdStorage.get(String componentName) = componentStorage -> one component in one
  //    topology's states for one checkpoint
  // componentStorage.get(Integer taskId) = the checkpoint for one task in one component
  private TreeMap<String, HashMap<String, HashMap<Integer, SimulatedCheckpoint>>> storage = new TreeMap<>();
  private TreeMap<String, Integer> checkpointIdCounts = new TreeMap<>();
  private Lock topologyLock = new ReentrantLock();
  private boolean lockedForRestore = false;
  private final int checkpointCount;

  /**
   * Initializes the state manager
   *
   * @param checkpointCount the number of instances that will be persisting their state.
   */
  public StateManager(int checkpointCount){
    this.checkpointCount = checkpointCount;
  }

  /**
   * Store a Checkpoint state
   *
   * @param checkpoint the checkpoint to store
   */
  public void store(SimulatedCheckpoint checkpoint) throws LockClosedException{
    boolean locked = topologyLock.tryLock();

    if(!locked){
      throw new LockClosedException();
    }

    try {
      storage.putIfAbsent(checkpoint.getCheckpointId(), new HashMap<>());

      Integer newCount = checkpointIdCounts.getOrDefault(checkpoint.getCheckpointId(), 0) + 1;

      if (newCount == checkpointCount) {
        this.disposeOlderThan(checkpoint.getCheckpointId(), false);
      }

      HashMap<String, HashMap<Integer, SimulatedCheckpoint>> checkpointIdStorage =
          storage.get(checkpoint.getCheckpointId());
      checkpointIdCounts.put(
          checkpoint.getCheckpointId(),
          newCount
      );

      checkpointIdStorage.putIfAbsent(checkpoint.getComponent(), new HashMap<>());

      HashMap<Integer, SimulatedCheckpoint> componentStorage =
          checkpointIdStorage.get(checkpoint.getComponent());

      componentStorage.put(checkpoint.getTaskId(), checkpoint);
    } finally {
      topologyLock.unlock();
    }
  }

  /**
   * Retrieve a Checkpoint state for a given checkpoint id and instance information
   *
   * @param checkpointId the checkpoint to retrieve from
   * @param componentName the name of this component
   * @param taskId the taskId of the state
   * @return the retrieved checkpoint
   */
  public SimulatedCheckpoint restore(String checkpointId, String componentName, Integer taskId) {
    try {
      return storage
          .get(checkpointId)
          .get(componentName)
          .get(taskId);
    } catch (NullPointerException e) {
      return null;
    }
  }

  /**
   * Alias of disposeOlderThan
   */
  public void dispose(String oldestCheckpointId, boolean deleteAll){
    this.disposeOlderThan(oldestCheckpointId, deleteAll);
  }

  /**
   * Delete all stored checkpoints
   */
  private void disposeAll(){
    storage = new TreeMap<>();
  }

  /**
   * Delete all stored checkpoints whose checkpoint ids are strictly less than oldestCheckpointId,
   * or all of them if deleteAll is true
   *
   * @param oldestCheckpointId the oldest checkpoint to keep (null sets deleteAll to true)
   * @param deleteAll true if all checkpoints should be deleted
   */
  private void disposeOlderThan(String oldestCheckpointId, boolean deleteAll){
    if(oldestCheckpointId == null){
      deleteAll = true;
    }

    if (deleteAll) {
      this.disposeAll();
    } else {
      storage = new TreeMap<>(storage.tailMap(oldestCheckpointId));
    }
  }

  /**
   * Delete all stored checkpoints whose checkpoint ids are strictly greater than
   * newestCheckpointId, or all of them if deleteAll is true
   *
   * @param newestCheckpointId the newest checkpoint to keep (null sets deleteAll to true)
   * @param deleteAll true if all checkpoints should be deleted
   */
  private void disposeNewerThan(String newestCheckpointId, boolean deleteAll){
    if(newestCheckpointId == null){
      deleteAll = true;
    }

    if (deleteAll) {
      this.disposeAll();
    } else {
      storage = new TreeMap<>(storage.headMap(newestCheckpointId));
    }
  }

  /**
   * Get the maximum checkpoint id which has a checkpointIdCount equal to checkpointCount
   *
   * @return the maximum checkpoint id, or null if none exists that satisfies the condition.
   */
  public String getMaxRestorableCheckpointId(){
    if (!lockedForRestore) {
      throw new RuntimeException("State Manager was not locked before restore process began.");
    }

    // Return the maximum checkpoint
    try {
      return checkpointIdCounts.lastKey();
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  public void lockAndResetForRestore(){
    topologyLock.lock();
    lockedForRestore = true;

    // Iterate through all entries in the map, starting with the most recent checkpoint id to
    // identify the most recent completed checkpoint, then clear out all checkpoints later than that
    for (Map.Entry<String, Integer> checkpointIdCount :
        checkpointIdCounts.descendingMap().entrySet()) {
      if (checkpointIdCount.getValue().equals(checkpointCount)) {
        this.disposeNewerThan(checkpointIdCount.getKey(), false);
        return;
      }
    }
  }

  public void unlockAfterRestored(){
    lockedForRestore = false;
    topologyLock.unlock();
  }

  public static class LockClosedException extends Exception{}
}
