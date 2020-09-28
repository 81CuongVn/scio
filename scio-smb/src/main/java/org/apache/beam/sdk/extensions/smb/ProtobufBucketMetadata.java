/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;

import java.util.Arrays;

public class ProtobufBucketMetadata<K, V extends Message> extends BucketMetadata<K, V> {

  @JsonProperty
  private final String keyField;

  @JsonIgnore
  private final String[] keyPath;

  private transient Descriptors.FieldDescriptor[] descriptorPath;

  public ProtobufBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K> keyClass,
      BucketMetadata.HashType hashType,
      String keyField,
      String filenamePrefix)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClass,
        hashType,
        keyField,
        filenamePrefix);
  }

  @JsonCreator
  ProtobufBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K> keyClass,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("keyField") String keyField,
      @JsonProperty(value = "filenamePrefix", required = false) String filenamePrefix)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    super(version, numBuckets, numShards, keyClass, hashType, filenamePrefix);
    this.keyField = keyField;
    this.keyPath = toKeyPath(keyField);
  }

  @SuppressWarnings("unchecked")
  @Override
  public K extractKey(V value) {
    if (descriptorPath == null) {
      Descriptors.FieldDescriptor[] path = new Descriptors.FieldDescriptor[keyPath.length];
      Message m = value;
      for (int i = 0; i < keyPath.length; i++) {
        String k = keyPath[i];
        Descriptors.FieldDescriptor curr = null;
        for (Descriptors.FieldDescriptor d : m.getAllFields().keySet()) {
          if (d.getName().equals(k)) {
            curr = d;
            break;
          }
        }
        if (curr == null) {
          throw new RuntimeException("Invalid key field " + keyField);
        }
        path[i] = curr;

        if (i != keyPath.length - 1) {
          m = (Message) m.getField(curr);
        }
      }
      descriptorPath = path;
      return (K) m.getField(descriptorPath[descriptorPath.length - 1]);
    } else {
      Message m = value;
      for (int i = 0; i < descriptorPath.length - 1; i++) {
        m = (Message) m.getField(descriptorPath[i]);
      }
      return (K) m.getField(descriptorPath[descriptorPath.length - 1]);
    }
  }

  @Override
  public boolean isPartitionCompatible(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProtobufBucketMetadata<?, ?> that = (ProtobufBucketMetadata<?, ?>) o;
    return getKeyClass() == that.getKeyClass()
        && keyField.equals(that.keyField)
        && Arrays.equals(keyPath, that.keyPath);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyField", keyField));
  }

  private static String[] toKeyPath(String keyField) {
    return keyField.split("\\.");
  }
}
