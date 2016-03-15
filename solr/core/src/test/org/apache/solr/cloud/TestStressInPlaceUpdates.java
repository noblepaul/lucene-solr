package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.primes.Primes;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.ValueSourceParser;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

@Slow
@SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
@SuppressSSL
public class TestStressInPlaceUpdates extends AbstractFullDistribZkTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeSuperClass() {
    System.setProperty("solr.commitwithin.softcommit", "false");
    schemaString = "schema16.xml";      // we need a docvalues based _version_
  }

  public TestStressInPlaceUpdates() {
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

  protected final ConcurrentHashMap<Integer,DocInfo> model = new ConcurrentHashMap<>();
  protected Map<Integer,DocInfo> committedModel = new HashMap<>();
  protected long snapshotCount;
  protected long committedModelClock;
  protected int clientIndexUsedForCommit;
  protected volatile int lastId;
  protected final String field = "val_l";

  private void initModel(int ndocs) {

    for (int i=0; i<ndocs; i++) {
      model.put(i, new DocInfo(0l, 0, 0));
    }
    committedModel.putAll(model);

  }
  @Test
  @ShardsFixed(num = 3)
  public void stressTest() throws Exception {
    waitForRecoveriesToFinish(true);

    final int commitPercent = 5 + random().nextInt(20);
    final int softCommitPercent = 30+random().nextInt(75); // what percent of the commits are soft
    final int deletePercent = 4+random().nextInt(25);
    final int deleteByQueryPercent = random().nextInt(8);
    final int ndocs = 5 + (random().nextBoolean() ? random().nextInt(25) : random().nextInt(200));
    int nWriteThreads = 5 + random().nextInt(25);
    int fullUpdatePercent = 5 + random().nextInt(50);

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

    // query variables
    final int percentRealtimeQuery = 75;
    final AtomicLong operations = new AtomicLong(200000);  // number of query operations to perform in total
    int nReadThreads = 5 + random().nextInt(25);


    /** // testing
    final int commitPercent = 5;
    final int softCommitPercent = 100; // what percent of the commits are soft
    final int deletePercent = 0;
    final int deleteByQueryPercent = 50;
    final int ndocs = 10;
    int nWriteThreads = 10;

    final int maxConcurrentCommits = nWriteThreads;   // number of committers at a time... it should be <= maxWarmingSearchers

    // query variables
    final int percentRealtimeQuery = 101;
    final AtomicLong operations = new AtomicLong(50000);  // number of query operations to perform in total
    int nReadThreads = 10;
    
    int fullUpdatePercent = 20;
     **/


    verbose("commitPercent",commitPercent, "softCommitPercent",softCommitPercent, "deletePercent",deletePercent, "deleteByQueryPercent",deleteByQueryPercent
        , "ndocs",ndocs,"nWriteThreads",nWriteThreads,"percentRealtimeQuery",percentRealtimeQuery,"operations",operations, "nReadThreads",nReadThreads);

    initModel(ndocs);

    final AtomicInteger numCommitting = new AtomicInteger();


    List<Thread> threads = new ArrayList<>();

    for (int i=0; i<nWriteThreads; i++) {
      Thread thread = new Thread("WRITER"+i) {
        Random rand = new Random(random().nextInt());

        @Override
        public void run() {
          try {
            while (operations.get() > 0) {
              int oper = rand.nextInt(100);

              if (oper < commitPercent) {
                if (numCommitting.incrementAndGet() <= maxConcurrentCommits) {
                  Map<Integer,DocInfo> newCommittedModel;
                  long version;

                  synchronized(this) {
                    newCommittedModel = new HashMap<>(model);  // take a snapshot
                    version = snapshotCount++;
                  }

                  int chosenClientIndex = rand.nextInt(clients.size());
                  
                  if (rand.nextInt(100) < softCommitPercent) {
                    verbose("softCommit start");
                    clients.get(chosenClientIndex).commit(true, true, true);
                    verbose("softCommit end");
                  } else {
                    verbose("hardCommit start");
                    commit();
                    clients.get(chosenClientIndex).commit();
                    verbose("hardCommit end");
                  }

                  synchronized(this) {

                    // install this model snapshot only if it's newer than the current one
                    if (version >= committedModelClock) {
                      if (VERBOSE) {
                        verbose("installing new committedModel version="+committedModelClock);
                      }
                      clientIndexUsedForCommit = chosenClientIndex;
                      committedModel = newCommittedModel;
                      committedModelClock = version;
                    }
                  }
                }
                numCommitting.decrementAndGet();
                continue;
              }


              int id;

              if (rand.nextBoolean()) {
                id = rand.nextInt(ndocs);
              } else {
                id = lastId;  // reuse the last ID half of the time to force more race conditions
              }

              // set the lastId before we actually change it sometimes to try and
              // uncover more race conditions between writing and reading
              boolean before = rand.nextBoolean();
              if (before) {
                lastId = id;
              }

              DocInfo info = model.get(id);

              
              // yield after getting the next version to increase the odds of updates happening out of order
              if (rand.nextBoolean()) Thread.yield();

              if (oper < commitPercent + deletePercent) {
                // TODO
              } else if (oper < commitPercent + deletePercent + deleteByQueryPercent) {
                // TODO
              } else {
                /*verbose("adding id", id, "val=", nextVal,"version",version);

                Long returnedVersion = addAndGetVersion(sdoc("id", Integer.toString(id), field, Long.toString(nextVal), "_version_",Long.toString(version)), params(DISTRIB_UPDATE_PARAM,FROM_LEADER));
                if (returnedVersion != null) {
                  assertEquals(version, returnedVersion.longValue());
                }

                // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (version > currInfo.version) {
                    model.put(id, new DocInfo(version, nextVal));
                  }
                }

                if (VERBOSE) {
                  verbose("adding id", id, "val=", nextVal,"version",version,"DONE");
                }*/
                
                
                int val1 = info.val1;
                long val2 = info.val2;
                int nextVal1 = val1;
                long nextVal2 = val2; 

                int partialUpdatePercent = 100-fullUpdatePercent;
                int addOper = rand.nextInt(100);
                long returnedVersion;
                if (addOper<fullUpdatePercent || info.version==0) {
                  // FULL UPDATE
                  nextVal1 = Primes.nextPrime(val1+1);
                  nextVal2 = nextVal1 * 1000000000l;
                  try {
                    returnedVersion = addDocAndGetVersion("id", id, "title_s", "title"+id, "val1_i_dvo", nextVal1, "val2_l_dvo", nextVal2, "_version_", info.version);
                    log.info("FULL: Writing id="+id+", val=["+nextVal1+","+nextVal2+"], version="+info.version+", Prev was=["+val1+","+val2+"].  Returned version="+returnedVersion);

                  } catch (RuntimeException e) {
                    if (e.getMessage()!=null && e.getMessage().contains("version conflict")) {
                      // Its okay for a leader to reject a concurrent request
                      log.warn("Conflict rejected id="+id+", "+e);
                      returnedVersion = -1;
                    } else throw e;
                  }
                } else { 
                  // PARTIAL
                  nextVal2 = val2 + val1;
                  try {
                    returnedVersion = addDocAndGetVersion("id", id, "val2_l_dvo", createMap("inc", String.valueOf(val1)), "_version_", info.version);
                    log.info("PARTIAL: Writing id="+id+", val=["+nextVal1+","+nextVal2+"], version="+info.version+", Prev was=["+val1+","+val2+"].  Returned version="+returnedVersion);
                  } catch (RuntimeException e) {
                    if (e.getMessage().contains("version conflict")) {
                      // Its okay for a leader to reject a concurrent request
                      log.warn("Conflict rejected id="+id+", "+e);
                      returnedVersion = -1;
                    } else throw e;
                  }
                }
                
             // only update model if the version is newer
                synchronized (model) {
                  DocInfo currInfo = model.get(id);
                  if (returnedVersion > currInfo.version) {
                    model.put(id, new DocInfo(returnedVersion, nextVal1, nextVal2));
                  }
                  
                }
              }

              if (!before) {
                lastId = id;
              }
            }
          } catch (Throwable e) {
            operations.set(-1L);
            log.error("",e);
            throw new RuntimeException(e);
          }
        }
      };

      threads.add(thread);
    }

    final AtomicInteger ctr = new AtomicInteger(0);
    // Read threads
    for (int i=0; i<nReadThreads; i++) {
      Thread thread = new Thread("READER"+i) {
        Random rand = new Random(random().nextInt());

        @Override
        public void run() {
          try {
            while (operations.decrementAndGet() >= 0) {
              // bias toward a recently changed doc
              int id = rand.nextInt(100) < 25 ? lastId : rand.nextInt(ndocs);

              // when indexing, we update the index, then the model
              // so when querying, we should first check the model, and then the index

              boolean realTime = rand.nextInt(100) < percentRealtimeQuery;
              DocInfo info;

              if (realTime) {
                info = model.get(id);
              } else {
                synchronized(this) {
                  info = committedModel.get(id);
                }
              }

              if (VERBOSE) {
                verbose("querying id", id);
              }
              ModifiableSolrParams params = new ModifiableSolrParams();
              if (realTime) {
                params.set("wt","json");
                params.set("qt","/get");
                params.set("ids",Integer.toString(id));
              } else {
                params.set("wt","json");
                params.set("q","id:"+Integer.toString(id));
                params.set("omitHeader","true");
              }

                int clientId = rand.nextInt(clients.size());
                if (!realTime) clientId = clientIndexUsedForCommit;
                
              QueryResponse response = clients.get(clientId).query(params);
              assertTrue("ZRQ, info="+info, info.version==0 || response.getResults().size()==1);
              if (response.getResults().size()==1) {
                assertNotNull("Realtime="+realTime+", Response is: "+response+", model: "+info,
                    response.getResults().get(0).get("val2_l_dvo"));
                
                Object obj1 = response.getResults().get(0).getFirstValue("val1_i_dvo");
                int val1 = obj1==null? 0:
                  ((obj1 instanceof ArrayList)? ((ArrayList<Integer>)obj1).get(0): (Integer)obj1);
                Object obj2 = response.getResults().get(0).getFirstValue("val2_l_dvo");
                long val2 = (obj2 instanceof ArrayList)? ((ArrayList<Long>)obj2).get(0): (Long)obj2;
                Object objVer= response.getResults().get(0).getFirstValue("_version_");
                long foundVer = (objVer instanceof ArrayList)? ((ArrayList<Long>)objVer).get(0): (Long)objVer;
                
                
                if (!(val1==0 && val2==0 || val2%val1==0)) {
                  assertTrue("Vals are: "+val1+", "+val2+", id="+id+ ", clientId="+clients.get(clientId)+", Doc retrived is: "+response.toString(), 
                      val1==0 && val2==0 || val2%val1==0);
                
                }
                if (foundVer < Math.abs(info.version)
                    || (foundVer == info.version && (val1 != info.val1 || val2 != info.val2)) ) {    // if the version matches, the val must
                  log.error("Realtime="+realTime+", ERROR, id=" + id + " found=" + response + " model=" + info);
                  assertTrue("Realtime="+realTime+", ERROR, id=" + id + " found=" + response + " model=" + info, false);
                }
              } else {
                //fail("Were were results: "+response);
              }
            }
          } catch (Throwable e) {
            operations.set(-1L);
            log.error("",e);
            throw new RuntimeException(e);
          }
        }
      };

      threads.add(thread);
    }
    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }
  }

  class DocInfo {
    long version;
    int val1;
    long val2;

    public DocInfo(long version, int val1, long val2) {
      this.version = version;
      this.val1 = val1;
      this.val2 = val2;
    }
    
    @Override
    public String toString() {
      return "["+version+", "+val1+", "+val2+"]";
    }
  }

  public static void verbose(Object... args) {
    // if (!log.isDebugEnabled()) return;
    StringBuilder sb = new StringBuilder("VERBOSE:");
    for (Object o : args) {
      sb.append(' ');
      sb.append(o==null ? "(null)" : o.toString());
    }
    log.info(sb.toString());
  }
  
  protected long addDocAndGetVersion(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    
    int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) % clients.size();
  
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("versions", "true");
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(params);
    ureq.add(doc);
    UpdateResponse resp;
    synchronized (cloudClient) {
      resp = ureq.process(clients.get(which));
    }
    
    return Long.parseLong(((NamedList)resp.getResponse().get("adds")).getVal(0).toString());
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
}
