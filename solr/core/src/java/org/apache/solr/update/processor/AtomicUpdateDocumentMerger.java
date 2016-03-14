package org.apache.solr.update.processor;

import java.lang.invoke.MethodHandles;

import java.io.IOException;

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

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.EnumField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @lucene.experimental
 */
public class AtomicUpdateDocumentMerger {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  protected final IndexSchema schema;
  protected final SchemaField idField;
  
  public AtomicUpdateDocumentMerger(SolrQueryRequest queryReq) {
    schema = queryReq.getSchema();
    idField = schema.getUniqueKeyField();
  }
  
  /**
   * Utility method that examines the SolrInputDocument in an AddUpdateCommand
   * and returns true if the documents contains atomic update instructions.
   */
  public static boolean isAtomicUpdate(final AddUpdateCommand cmd) {
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    for (SolrInputField sif : sdoc.values()) {
      if (sif.getValue() instanceof Map) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Merges the fromDoc into the toDoc using the atomic update syntax.
   * 
   * @param fromDoc SolrInputDocument which will merged into the toDoc
   * @param toDoc the final SolrInputDocument that will be mutated with the values from the fromDoc atomic commands
   * @return toDoc with mutated values
   */
  public SolrInputDocument merge(final SolrInputDocument fromDoc, SolrInputDocument toDoc) {
    for (SolrInputField sif : fromDoc.values()) {
     Object val = sif.getValue();
      if (val instanceof Map) {
        for (Entry<String,Object> entry : ((Map<String,Object>) val).entrySet()) {
          String key = entry.getKey();
          Object fieldVal = entry.getValue();
          boolean updateField = false;
          switch (key) {
            case "add":
              updateField = true;
              doAdd(toDoc, sif, fieldVal);
              break;
            case "set":
              updateField = true;
              doSet(toDoc, sif, fieldVal);
              break;
            case "remove":
              updateField = true;
              doRemove(toDoc, sif, fieldVal);
              break;
            case "removeregex":
              updateField = true;
              doRemoveRegex(toDoc, sif, fieldVal);
              break;
            case "inc":
              updateField = true;
              doInc(toDoc, sif, fieldVal);
              break;
            default:
              //Perhaps throw an error here instead?
              log.warn("Unknown operation for the an atomic update, operation ignored: " + key);
              break;
          }
          // validate that the field being modified is not the id field.
          if (updateField && idField.getName().equals(sif.getName())) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid update of id field: " + sif);
          }

        }
      } else {
        // normal fields are treated as a "set"
        toDoc.put(sif.getName(), sif);
      }
    }
    
    return toDoc;
  }

  private boolean isSupportedForInPlaceUpdate(SchemaField field) {
    if (!field.hasDocValues() || field.multiValued())
      return false;
    
    FieldType type = field.getType();
    if (!(type instanceof TrieIntField || type instanceof TrieLongField ||
        type instanceof TrieFloatField || type instanceof TrieDoubleField)) {
      return false;
    }
    return true;
  }

  public boolean doInPlaceUpdateMerge(AddUpdateCommand cmd, SolrInputDocument sdoc, BytesRef id) throws IOException {

    IndexSchema schema = cmd.getReq().getSchema();

    // Whether this update command has updates to a supported field. A supported update requires the value be a map.
    boolean needsUpdate = false;
    
    for (String field : sdoc.getFieldNames()) {
      if (idField.getName().equals(field))
        continue;
      SchemaField schemaField = schema.getField(field);
      if (schema.getField(field).indexed() || schema.getField(field).stored()) {
        return false;
      } else if (!isSupportedForInPlaceUpdate(schemaField)) {
        return false;
      } else if (sdoc.getField(field).getValue() instanceof Map) {
        needsUpdate = true;
      }
    }
    if (!needsUpdate) {
      // This is not an inplace update
      return false;
    }

    SolrInputDocument dvDoc = new SolrInputDocument();

    SolrInputDocument uncommittedDoc = RealTimeGetComponent.getInputDocumentFromTlog(cmd.getReq().getCore(), id);
    log.info("Uncommitted doc is: " + uncommittedDoc);

    Long oldVersion = null;
    // Need to get a docid for the document, so that numeric docvalue can be updated
    RefCounted<SolrIndexSearcher> searcherHolder = cmd.getReq().getCore().getRealtimeSearcher();
    try {
      SolrIndexSearcher searcher = searcherHolder.get();
      int docId = searcher.getFirstMatch(new Term(idField.getName(), id));
      if (uncommittedDoc != null) {
        oldVersion = Long.parseLong(uncommittedDoc.get(DistributedUpdateProcessor.VERSION_FIELD).getFirstValue().toString());
      } else {
        oldVersion = RealTimeGetComponent.getVersionFromTlog(cmd.getReq().getCore(), id);
      }

      for (String fieldName : sdoc.getFieldNames()) {
        SchemaField schemaField = schema.getField(fieldName);
        if (schemaField.hasDocValues() && (schemaField.getType() instanceof TrieField)) {
          Object fieldValue = sdoc.getField(fieldName).getValue();
          if (fieldValue instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) fieldValue;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
              String key = entry.getKey();
              Object value = entry.getValue();

              log.info("Atomic merge of: " + entry);
              switch (key) {
                case "set":
                  dvDoc.addField(sdoc.getField(fieldName).getName(),
                      schemaField.getType().toNativeType(value), sdoc.getField(fieldName).getBoost());
                  break;
                case "inc":
                  Object oldValue = null;
                  if (uncommittedDoc != null) {
                    if (uncommittedDoc.containsKey(fieldName)) {
                      oldValue = uncommittedDoc.get(fieldName).getValue();
                    }
                  }
                  if (oldValue == null && docId >= 0) { //look in the index
                    NumericDocValues ndv = searcher.getLeafReader().getNumericDocValues(fieldName);
                    if (ndv != null) {
//                      oldValue = ndv.get(docId);
                      
                      Long val = ndv.get(docId);
                      oldValue = val;
                      if (schemaField.getType() instanceof TrieIntField) {
                        oldValue = val.intValue();
                      } else if (schemaField.getType() instanceof TrieFloatField) {
                        oldValue = NumericUtils.sortableIntToFloat(val.intValue());
                      } else if (schemaField.getType() instanceof TrieDoubleField) {
                        oldValue = NumericUtils.sortableLongToDouble(val);
                      }                    }
                  }
                  if (oldValue == null) { // if not found in index too, use docvalue field's default value
                    oldValue = schemaField.getDefaultValue();
                    log.warn("No previous value for field '" + fieldName + "' found for document id '"
                        + cmd.getPrintableId() + "', operation 'inc', value '" + value + "'.");
                  }

                  Number result;
                  oldValue = schemaField.getType().toNativeType(oldValue);

                  log.info("INC operation: oldVersion: " + oldVersion + ", oldValue: " + oldValue + ", value=" + value + ", id=" + cmd.getPrintableId());

                  if (oldValue instanceof Long) {
                    result = ((Long) oldValue).longValue() + Long.parseLong(value.toString());
                  } else if (oldValue instanceof Float) {
                    result = ((Float) oldValue).floatValue() + Float.parseFloat(value.toString());
                  } else if (oldValue instanceof Double) {
                    result = ((Double) oldValue).doubleValue() + Double.parseDouble(value.toString());
                  } else {
                    // int, short, byte
                    result = ((Integer) oldValue).intValue() + Integer.parseInt(value.toString());
                  }

                  dvDoc.addField(sdoc.getField(fieldName).getName(), result, sdoc.getField(fieldName).getBoost());
                  break;
                default:
                  log.warn("Unsupported operation '" + key + "' for inplace update.");
                  break;
              }
            }
          } else { // for version field, which is a docvalue but there's no set/inc operation
            dvDoc.addField(sdoc.getField(fieldName).getName(), fieldValue, sdoc.getField(fieldName).getBoost());
          }
        } else { // for id field
          dvDoc.addField(sdoc.getField(fieldName).getName(), sdoc.getField(fieldName).getValue(), sdoc.getField(fieldName).getBoost());
        }
      }

    } finally {
      if (searcherHolder != null) {
        searcherHolder.decref();
      }
    }

    // Now include all previously uncommitted ndv partial updates to this dvDoc partial document
    if (uncommittedDoc != null) {
      for (String fieldName : uncommittedDoc.getFieldNames()) {
        SchemaField schemaField = schema.getField(fieldName);
        if (dvDoc.containsKey(fieldName) == false && schemaField.hasDocValues() && (schemaField.getType() instanceof TrieField)) {
          dvDoc.addField(fieldName, uncommittedDoc.getFieldValue(fieldName));
        }
      }
    }

    cmd.prevVersion = oldVersion;
    cmd.solrDoc = dvDoc;
    cmd.isInPlaceUpdate = true;
    return true;
  }

  protected void doSet(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    SchemaField sf = schema.getField(sif.getName());
    toDoc.setField(sif.getName(), sf.getType().toNativeType(fieldVal), sif.getBoost());
  }

  protected void doAdd(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    SchemaField sf = schema.getField(sif.getName());
    toDoc.addField(sif.getName(), sf.getType().toNativeType(fieldVal), sif.getBoost());
  }

  protected void doInc(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    SolrInputField numericField = toDoc.get(sif.getName());
    if (numericField == null) {
      toDoc.setField(sif.getName(),  fieldVal, sif.getBoost());
    } else {
      // TODO: fieldtype needs externalToObject?
      String oldValS = numericField.getFirstValue().toString();
      SchemaField sf = schema.getField(sif.getName());
      BytesRefBuilder term = new BytesRefBuilder();
      sf.getType().readableToIndexed(oldValS, term);
      Object oldVal = sf.getType().toObject(sf, term.get());

      String fieldValS = fieldVal.toString();
      Number result;
      if (oldVal instanceof Long) {
        result = ((Long) oldVal).longValue() + Long.parseLong(fieldValS);
      } else if (oldVal instanceof Float) {
        result = ((Float) oldVal).floatValue() + Float.parseFloat(fieldValS);
      } else if (oldVal instanceof Double) {
        result = ((Double) oldVal).doubleValue() + Double.parseDouble(fieldValS);
      } else {
        // int, short, byte
        result = ((Integer) oldVal).intValue() + Integer.parseInt(fieldValS);
      }

      toDoc.setField(sif.getName(),  result, sif.getBoost());
    }
  }

  protected void doRemove(SolrInputDocument toDoc, SolrInputField sif, Object fieldVal) {
    final String name = sif.getName();
    SolrInputField existingField = toDoc.get(name);
    if (existingField == null) return;
    SchemaField sf = schema.getField(name);

    if (sf != null) {
      final Collection<Object> original = existingField.getValues();
      if (fieldVal instanceof Collection) {
        for (Object object : (Collection) fieldVal) {
          Object o = sf.getType().toNativeType(object);
          original.remove(o);
        }
      } else {
        original.remove(sf.getType().toNativeType(fieldVal));
      }

      toDoc.setField(name, original);
    }
  }

  protected void doRemoveRegex(SolrInputDocument toDoc, SolrInputField sif, Object valuePatterns) {
    final String name = sif.getName();
    final SolrInputField existingField = toDoc.get(name);
    if (existingField != null) {
      final Collection<Object> valueToRemove = new HashSet<>();
      final Collection<Object> original = existingField.getValues();
      final Collection<Pattern> patterns = preparePatterns(valuePatterns);
      for (Object value : original) {
        for(Pattern pattern : patterns) {
          final Matcher m = pattern.matcher(value.toString());
          if (m.matches()) {
            valueToRemove.add(value);
          }
        }
      }
      original.removeAll(valueToRemove);
      toDoc.setField(name, original);
    }
  }

  private Collection<Pattern> preparePatterns(Object fieldVal) {
    final Collection<Pattern> patterns = new LinkedHashSet<>(1);
    if (fieldVal instanceof Collection) {
      Collection<String> patternVals = (Collection<String>) fieldVal;
      for (String patternVal : patternVals) {
        patterns.add(Pattern.compile(patternVal));
      }
    } else {
      patterns.add(Pattern.compile(fieldVal.toString()));
    }
    return patterns;
  }
  
}
