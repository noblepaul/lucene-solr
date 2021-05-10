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

package org.apache.solr.loader;


import java.lang.invoke.MethodHandles;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.common.util.DataEntry;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.DoublePointField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.FloatPointField;
import org.apache.solr.schema.IntPointField;
import org.apache.solr.schema.LongPointField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastFieldReaders {
  private static final BytesRef TRUE = new BytesRef("true");
  private static final BytesRef FALSE = new BytesRef("false");
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final Map<Class<?extends FieldType>, FieldsCollector> readers = new ImmutableMap.Builder<Class<?extends FieldType>, FieldsCollector>()
      .put(StrField.class, new StrType())
      .put(IntPointField.class, new IntType())
      .put(LongPointField.class, new LongType())
      .put(FloatPointField.class, new FloatType())
      .put(DoublePointField.class, new DoubleType())
      .put(BoolField.class, new BoolType())
      .build();


  public static FieldsCollector getInst(FieldType type) {
    return readers.get(type.getClass());
  }

  static class StrType implements FieldsCollector {
    @Override
    public void collectFields(FieldCtx ctx) {
      ByteArrayUtf8CharSequence utf8 = (ByteArrayUtf8CharSequence) ctx.data.val();
      BytesRef bytesRef = new BytesRef(utf8.getBuf(), utf8.offset(), utf8.length());
      if(!ignore(ctx.field))
      ctx.document.add(new StringField(ctx.field.getName(), bytesRef, ctx.field.store()));
      if (ctx.field.hasDocValues()) {
        if (ctx.data.type() == DataEntry.Type.STR) {
          if (ctx.field.multiValued()) {
            ctx.document.add(new SortedSetDocValuesField(ctx.field.getName(), bytesRef));
          } else {
            ctx.document.add(new SortedDocValuesField(ctx.field.getName(), bytesRef));
          }
        }
      }
    }
  }
  static class BoolType implements FieldsCollector {
    @Override
    public void collectFields(FieldCtx ctx) {
      if(ctx.field.indexed()) ctx.document.add(ctx.field.getType().createField(ctx.field, ctx.boolVal()? "true": "false"));
      if(ctx.field.hasDocValues()) {
        if (ctx.field.multiValued()) {
          ctx.document.add(new SortedSetDocValuesField(ctx.field.getName(), ctx.boolVal()? TRUE: FALSE));
        } else {
          ctx.document.add(new SortedDocValuesField(ctx.field.getName(), ctx.boolVal()? TRUE: FALSE));
        }
      }
    }
  }
  static class DoubleType extends PointType {
    @Override
    IndexableField indexedField(FieldCtx ctx) { return new DoublePoint(ctx.field.getName(), ctx.doubleVal()); }

    @Override
    StoredField storedField(FieldCtx ctx) { return new StoredField(ctx.field.getName(), ctx.doubleVal()); }

  }
  static class FloatType extends PointType {

    @Override
    IndexableField indexedField(FieldCtx ctx) { return new FloatPoint(ctx.field.getName(), ctx.floatVal()); }

    @Override
    StoredField storedField(FieldCtx ctx) { return new StoredField(ctx.field.getName(), ctx.floatVal()); }
  }
  static class LongType extends PointType {

    @Override
    IndexableField indexedField(FieldCtx ctx) { return new LongPoint(ctx.field.getName(), ctx.longVal());}

    @Override
    StoredField storedField(FieldCtx ctx) { return new StoredField(ctx.field.getName(), ctx.longVal()); }
  }

  static class IntType extends PointType {
    @Override
    IndexableField indexedField(FieldCtx ctx) { return new IntPoint(ctx.field.getName(), ctx.intVal()); }

    @Override
    StoredField storedField(FieldCtx ctx) { return new StoredField(ctx.field.getName(), ctx.intVal()); }
  }

  static abstract class PointType implements FieldsCollector {

    abstract IndexableField indexedField(FieldCtx ctx);
    abstract StoredField storedField(FieldCtx ctx);

    @Override
    public void collectFields(FieldCtx ctx) {
      if (ctx.field.indexed()) ctx.document.add(indexedField(ctx));
      if (ctx.field.hasDocValues()) {
        if (ctx.field.multiValued()) {
          ctx.document.add(new NumericDocValuesField(ctx.field.getName(), ctx.longVal()));
        } else {
          ctx.document.add(new SortedNumericDocValuesField(ctx.field.getName(), ctx.longVal()));
        }
      }
      if (ctx.field.stored()) ctx.document.add(storedField(ctx));
    }
  }

  private static boolean ignore(SchemaField field) {
    if (!field.indexed() && !field.stored()) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: {}", field);
      return true;
    } else {
      return false;
    }
  }

}
