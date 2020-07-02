package org.apache.solr.common.assign;


import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionParams;

import java.util.List;
import java.util.Map;

public interface ReplicaAssignStrategy {

    void init(SolrCloudManager cloudManager);

    List<Operation> getOperations(List<Intent> actions);


    interface Operation {
        String endPoint();
        MapWriter payload();

    }

    interface AddOperation extends Operation {
        default IntentType collectionAction(){
            return IntentType.ADD;
        }
        String targetNode();
        String collection();
        String slice();
        Replica.Type type();


    }

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


    enum IntentType {
        ADD(CollectionParams.CollectionAction.ADDREPLICA),
        MOVE(CollectionParams.CollectionAction.MOVEREPLICA);

        final CollectionParams.CollectionAction action;

        IntentType(CollectionParams.CollectionAction action) {
            this.action = action;
        }
    }

}
