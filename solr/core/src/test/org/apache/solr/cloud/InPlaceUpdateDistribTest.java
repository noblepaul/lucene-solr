package org.apache.solr.cloud;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
@SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
@SuppressSSL
public class InPlaceUpdateDistribTest extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeSuperClass() {
    System.setProperty("solr.commitwithin.softcommit", "false");
    schemaString = "schema16.xml";
  }

  public InPlaceUpdateDistribTest() {
    super();
    sliceCount = 1;
    fixShardCount(3);

    // A value source parser that returns the underlying lucene docid from the segment in which the document resides.
    // This can give a sense of whether a new document was added or the same document was updated?
    ValueSourceParser luceneDocidVSP = new ValueSourceParser() {
      @Override
      public ValueSource parse(FunctionQParser fp) throws SyntaxError {
        return new ValueSource() {
          @Override
          public int hashCode() {
            return 0;
          }

          @Override
          public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
            return new IntDocValues(this) {
              @Override
              public int intVal(int doc) {
                return (doc);
              }

              @Override
              public long longVal(int doc) {
                return (doc);
              }
            };
          }

          @Override
          public boolean equals(Object o) {
            return false;
          }

          @Override
          public String description() {
            return null;
          }
        };
      }
    };

    ValueSourceParser.addParser("lucenedoc", luceneDocidVSP);
  }

  @Test
  @ShardsFixed(num = 3)
  public void test() throws Exception {
    waitForRecoveriesToFinish(true);
    docValuesUpdateTest();
    ensureRtgWorksWithPartialUpdatesTest();
    delayedReorderingFetchesMissingUpdateFromLeaderTest();
    outOfOrderUpdatesIndividualReplicaTest(); // perform this test last, since this will throw the replicas out of sync
  }

  private void docValuesUpdateTest() throws Exception,
      IOException {
    int numDocs = 10;

    for (int i = 0; i < numDocs; i++)
      index("id", i, t1, "title" + i, "id_i", i);
    commit();

    List<Long> luceneDocids = new ArrayList<Long>();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fl", "id,ratings,title,price,_version_,field(ratings),lucenedoc()");
    params.add("rows", String.valueOf(numDocs));
    params.add("sort", "id_i asc");
    SolrDocumentList results = clients.get(random().nextInt(clients.size())).query(params).getResults();
    assertEquals(numDocs, results.size());
    for (SolrDocument doc : results) {
      luceneDocids.add(Long.parseLong(doc.get("lucenedoc()").toString()));
    }
    log.info("Results 1: "+results);

    List<Float> ratingsList = new ArrayList<Float>();
    for (int i = 0; i < numDocs; i++)
      ratingsList.add(r.nextFloat()*5.0f);
    log.info("Ratings: "+ratingsList);
    // update doc, set
    for (int i = numDocs - 1; i >= 0; i--) {
      index("id", i, "ratings", createMap("set", (String.valueOf(ratingsList.get(i)))));
    }
    
    commit();
    Thread.sleep(500); // wait for the commit to be distributed
    
    results = clients.get(random().nextInt(clients.size())).query(params).getResults();
    assertEquals(numDocs, results.size());
    log.info("Results 2: "+results);

    int counter = 0;
    for (SolrDocument doc : results) {
      long v = Long.parseLong(doc.get("_version_").toString());
      float r = Float.parseFloat(doc.get("ratings").toString());
      int l = Integer.parseInt(doc.get("lucenedoc()").toString());

      assertEquals(luceneDocids.get(counter).longValue(), l);
      assertEquals(ratingsList.get(counter).floatValue(), r, 0.01);
      counter++;
    }

    // update doc, increment
    for (int i = 0; i < numDocs; i++) {
      int inc = r.nextInt(1000) * (r.nextBoolean() ? -1 : 1);
      ratingsList.set(i, ratingsList.get(i) + inc);
      index("id", i, "ratings", createMap("inc", ("" + inc)));
    }
    commit();;
    Thread.sleep(500); // wait for the commit to be distributed

    log.info("Ratings 3: "+ratingsList);
    results = clients.get(random().nextInt(clients.size())).query(params).getResults();
    assertEquals(numDocs, results.size());
    log.info("Results 3: "+results);

    counter = 0;
    for (SolrDocument doc : results) {
      long v = Long.parseLong(doc.get("_version_").toString());
      float r = Float.parseFloat(doc.get("ratings").toString());
      int l = Integer.parseInt(doc.get("lucenedoc()").toString());

      assertEquals(luceneDocids.get(counter).longValue(), l);
      assertEquals(ratingsList.get(counter).floatValue(), r, 0.01);
      counter++;
    }
  }

  private void ensureRtgWorksWithPartialUpdatesTest() throws Exception,
      IOException {
    index("id", 0, t1, "title0", "id_i", 0);
    commit();;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fl", "id,ratings,_version_,lucenedoc()");

    float ratings = 1;

    // update doc, set
    index("id", 0, "ratings", createMap("set", (String.valueOf(ratings))));
    commit();
    SolrDocument sdoc = clients.get(random().nextInt(clients.size())).getById("0");  // RTG straight from the index
    log.info("FIRST: " + sdoc);
    assertEquals((float) ratings, sdoc.get("ratings"));
    assertEquals("title0", sdoc.get(t1));

    index("id", 0, t1, "title1", "ratings", ratings); // full indexing

    ratings++;
    index("id", 0, "ratings", createMap("inc", "1"));
    index("id", 0, "price", createMap("set", "100"));

    ratings++;
    index("id", 0, "ratings", createMap("inc", "1"));
    sdoc = clients.get(random().nextInt(clients.size())).getById("0");
    log.info("SECOND: " + sdoc);

    assertEquals((int) 100, sdoc.get("price"));
    assertEquals((float) ratings, sdoc.get("ratings"));
    assertEquals("title1", sdoc.get(t1));
  }
  
  private void outOfOrderUpdatesIndividualReplicaTest() throws Exception,
      IOException {
    index("id", 0, t1, "title0", "id_i", 0);
    commit();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fl", "id,field(ratings),field(_version_),lucenedoc()");

    float ratings = 1;

    // update doc, set
    index("id", 0, "ratings", createMap("set", (String.valueOf(ratings))));

    commit();
    SolrDocument sdoc = clients.get(random().nextInt(clients.size())).getById("0");  // RTG straight from the index
    assertEquals(ratings, sdoc.get("ratings"));
    assertEquals("title0", sdoc.get(t1));
    long version0 = Long.parseLong(sdoc.get("_version_").toString());

    // put replica out of sync
    float newRatings = 100;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(simulatedUpdateRequest(null, "id", 0, t1, "title0_new", "ratings", "" + newRatings, "_version_", version0 + 1)); // full update
    updates.add(simulatedUpdateRequest(version0 + 1, "id", 0, "ratings", newRatings + 1, "_version_", version0 + 2)); // ratings=101
    updates.add(simulatedUpdateRequest(version0 + 2, "id", 0, "ratings", newRatings + 2, "_version_", version0 + 3)); // ratings=102

    // order the updates correctly for replica3
    for (UpdateRequest update : updates) {
      log.info("Issuing well ordered update: " + update.getDocuments());
      clients.get(2).request(update);
    }

    ExecutorService threadpool = Executors.newFixedThreadPool(3);

    // re-order the updates for replica2
    List<UpdateRequest> reorderedUpdates = new ArrayList<>(updates);
    Collections.shuffle(reorderedUpdates, random());
    for (UpdateRequest update : reorderedUpdates) {
      SendUpdateToReplicaTask task = new SendUpdateToReplicaTask(update, clients.get(1), random());
      threadpool.submit(task);
    }

    threadpool.awaitTermination(2, TimeUnit.SECONDS);
    threadpool.shutdown();

    // assert both replicas have same effect
    for (int i : new int[]{1, 2}) { // 1 is re-ordered replica, 2 is well-ordered replica
      log.info("Testing client: " + i);
      assertReplicaValue(i, 0, "ratings", "" + (newRatings + 2.0f));
      assertReplicaValue(i, 0, t1, "title0_new");
      assertEquals(version0 + 3, Long.parseLong(getReplicaValue(i, 0, "_version_")));
    }

    log.info("This test passed fine...");
  }
  
  private void delayedReorderingFetchesMissingUpdateFromLeaderTest() throws Exception,
  IOException {
    float ratings = 1;

    index("id", 1, t1, "title1", "id_i", 1, "ratings", String.valueOf(ratings));
    commit();

    float newRatings = 100;
    List<UpdateRequest> updates = new ArrayList<>();
    updates.add(regularUpdateRequest("id", 1, t1, "title1_new", "id_i", 1, "ratings", String.valueOf(newRatings)));
    updates.add(regularUpdateRequest("id", 1, "ratings", createMap("inc", String.valueOf(1))));
    updates.add(regularUpdateRequest("id", 1, "ratings", createMap("inc", String.valueOf(1))));


    // The next request to replica2 will be delayed by 2 secs 
    shardToJetty.get(SHARD1).get(1).jetty.getDebugFilter().setDelay(1, 2000);

    ExecutorService threadpool = Executors.newFixedThreadPool(3);
    for (UpdateRequest update : updates) {
      SendUpdateToReplicaTask task = new SendUpdateToReplicaTask(update, cloudClient, random());
      threadpool.submit(task);
      Thread.sleep(100); // so as to ensure requests are sequential, even though from separate threads
    }

    threadpool.awaitTermination(2, TimeUnit.SECONDS);
    threadpool.shutdown();

    commit();

    Thread.sleep(500); // wait a bit for LIR to kick in
    cloudClient.getZkStateReader().updateClusterState();
    ClusterState state = cloudClient.getZkStateReader().getClusterState();
    
    int numActiveReplicas = 0;
    for (Replica rep: state.getSlice("collection1", SHARD1).getReplicas())
      if (rep.getState().equals(Replica.State.ACTIVE))
        numActiveReplicas++;

    //assertEquals(2, numActiveReplicas);
    assertEquals("The replica receiving reordered updates mustn't have gone down", 3, numActiveReplicas);

    // Verify that eventually, replicas recover and attain steady state
    waitForRecoveriesToFinish(true);

    //commit("softCommit","true");;

    /*cloudClient.getZkStateReader().updateClusterState();
    state = cloudClient.getZkStateReader().getClusterState();
    log.info("State is now: "+state.getSlice("collection1", SHARD1));*/
    
    for (int i : new int[] {0, 1, 2}) { // 0 is leader, 1 re-ordered replica, 2 well-ordered replica
      log.info("Testing client (LIR test): " + i);
      log.info("Version at "+i+" is: "+getReplicaValue(i, 1, "_version_"));

      assertReplicaValue(i, 1, "ratings", "" + (newRatings + 2.0f));
      assertReplicaValue(i, 1, t1, "title1_new");
    }

    log.info("This test passed fine...");
  }

  private static class SendUpdateToReplicaTask implements Callable {
    UpdateRequest update;
    SolrClient solrClient;
    Random rnd;

    public SendUpdateToReplicaTask (UpdateRequest update, SolrClient solrClient, Random rnd) {
      this.update = update;
      this.solrClient = solrClient;
      this.rnd = rnd;
    }

    @Override
    public Object call() throws Exception {
      solrClient.request(update);
      if (random().nextInt(3) == 0)
        solrClient.commit();;
      return true;
    }
  }
  
  

  String getReplicaValue(int clientIndex, int doc, String field) throws SolrServerException, IOException {
    ModifiableSolrParams distributedFalseParams = new ModifiableSolrParams();
    distributedFalseParams.set("distrib", "false");
    SolrDocument sdoc = clients.get(clientIndex).getById("" + doc, distributedFalseParams);
    return sdoc.get(field).toString();
  }

  void assertReplicaValue(int clientIndex, int doc, String field, String expected) throws SolrServerException, IOException {
    assertEquals(expected, getReplicaValue(clientIndex, doc, field));
  }

  UpdateRequest simulatedUpdateRequest(Long prevVersion, Object... fields) throws SolrServerException, IOException {
    String baseUrl = ((HttpSolrClient) shardToJetty.get(SHARD1).get(0).client.solrClient).getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    UpdateRequest ur = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    ur.add(doc);
    ur.setParam("update.distrib", "FROMLEADER");
    if (prevVersion != null) {
      ur.setParam("distrib.inplace.prevversion", String.valueOf(prevVersion));
      ur.setParam("distrib.inplace.update", "true");
    }
    ur.setParam("distrib.from", baseUrl + "collection1/"); //ZkCoreNodeProps.getCoreUrl(baseUrl, leader.getName()));
    return ur;
  }
  
  UpdateRequest regularUpdateRequest(Object... fields) throws SolrServerException, IOException {
    UpdateRequest ur = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    ur.add(doc);
    return ur;
  }


  /**
   * Strings at even index are keys, odd-index strings are values in the
   * returned map
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Map createMap(Object... args) {
    Map result = new LinkedHashMap();

    if (args == null || args.length == 0)
      return result;

    for (int i = 0; i < args.length - 1; i += 2)
      result.put(args[i], args[i + 1]);

    return result;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  protected SolrInputDocument addRandFields(SolrInputDocument sdoc) {
    return sdoc;
  }
}