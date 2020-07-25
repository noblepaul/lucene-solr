/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cloud.api.collections;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.solr.common.cloud.Replica;

/**
 * Interface implemented by contributed assign strategies.
 */
public interface IlanProposalAssigner {

  List<AssignmentDecision> computeAssignements(AssignerClusterState state, List<AssignementRequest> requests);


  /**
   * Initial cluster state for assigner code.
   *
   * Here and other interfaces, some Iterator better off as Set or Collection if we're likely to iterate over all values
   */
  interface AssignerClusterState {
    Set<String> getNodeNames();
    Set<String> getLiveNodeNames();
    Iterator<String> getCollectionNames();

    AssignerNodeState getNodeState(String nodeName);
    AssignerCollectionState getCollectionState(String collectionName);
  }

  interface AssignerNodeState {
    Iterator<AssignerReplicaInfo> getReplicasInfo();

    int getCountReplicas();
    long getTotalSizeBytes();
    long getFreeSizeBytes();
  }

  interface AssignerReplicaInfo {
    String getReplicaName();
    String getCollectionName();
    String getNodeName();
    String getSliceName();
    Replica.Type getReplicaType();
  }

  interface AssignerCollectionState {
    String getCollectionName();
    Iterator<String> getSliceNames();

    Iterator<AssignerReplicaInfo> getSliceReplicasInfo(String sliceName);
  }

  interface AssignementRequest {
  }

  interface AddReplicaAssignmentRequest {
    String getCollectionName();
    String getSliceName();
    Replica.Type getType();
  }

  interface DeleteReplicaAssignmentRequest {
    String getCollectionName();
    String getSliceName();
    Replica.Type getType();
  }

  interface MoveReplicaAssignmentRequest {
    String getSourceNodeName();
    String getCollectionName();
    String getSliceName();
    Replica.Type getType();
  }

  interface AssignmentDecision {
    // could move some methods here but added value seems low
  }

  interface AddReplicaAssignmentDecision extends AssignmentDecision {
    String getNodeName();
    String getCollectionName();
    String getSliceName();
    Replica.Type getType();
  }

  interface MoveReplicaAssignmentDecision extends AssignmentDecision {
    String getSourceNodeName();
    String getTargetNodeName();
    String getCollectionName();
    String getSliceName();
    String getReplicaName();
    Replica.Type getReplicaType();
  }

  interface DeleteReplicaAssignmentDecision extends AssignmentDecision {
    String getNode();
    String getCollection();
    String getSlice();
    String getReplicaName();
    Replica.Type getType();
  }

}
