package org.apache.solr.common.assign;


import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionParams;

import java.util.List;
import java.util.Map;

/**
 * Every implementation of assign-strategy should implemenmt this interface
 * A new instance is created just in time for computing operations. This instance will
 * be thrown away after the computations are over
 *
 */
public interface ReplicaAssignStrategy {

    /**
     * This method is invoked before anything else is done.
     * choose to read any configuartion that may be stored in ZK etc.
     * @param cloudManager
     */
    void init(SolrCloudManager cloudManager);

    /**
     * get a list of operations that can be performed for the intents.
     *
     * There should be a one-to-one mapping between intent and operation
     *
     */
    List<Operation> getOperations(List<Intent> intents);


    interface Operation {
        /**Generic method invocation endpoint for v2 APIs
         *
         * @return
         */
        String endPoint();

        /** The payload. This will be serialized to JSON and will be psosted to SOlr
         *
         * @return
         */
        MapWriter payload();

        /**The intent associated with the operation
         */
        Intent getIntent();

    }


    /**
     * This operation adds a replica to a given collection/shard
     */
    interface AddOperation extends Operation {
        default IntentType collectionAction(){
            return IntentType.ADD;
        }
        String targetNode();
        String collection();
        String slice();
        Replica.Type type();

    }

    /**
     * This operation does move a replica from one node to another
     */
    interface MoveOperation extends AddOperation {
        default  IntentType collectionAction(){
            return IntentType.MOVE;
        }
        String fromNode();
        String replicaName();
    }


    class Intent {
        IntentType intentType;
        Map<Suggester.Hint, Object> hints;
        String collection;
        String shard;
        Replica.Type replicaType;
    }


    /**
     * Supported Intent types
     */
    enum IntentType {
        /**
         * Adds a replica
         */
        ADD(CollectionParams.CollectionAction.ADDREPLICA),
        /**
         * Moves a replica
         */
        MOVE(CollectionParams.CollectionAction.MOVEREPLICA);

        final CollectionParams.CollectionAction action;

        IntentType(CollectionParams.CollectionAction action) {
            this.action = action;
        }
    }

}
