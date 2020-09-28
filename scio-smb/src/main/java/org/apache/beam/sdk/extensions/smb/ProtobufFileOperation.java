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

import com.google.protobuf.Message;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.PatchedSerializableAvroCodecFactory;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.MimeTypes;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.NoSuchElementException;

public class ProtobufFileOperation<ValueT extends Message> extends FileOperations<ValueT> {
  private final Class<ValueT> recordClass;
  private final PatchedSerializableAvroCodecFactory codec;
  private final Map<String, Object> metadata;

  private ProtobufFileOperation(
      Class<ValueT> recordClass, CodecFactory codec, Map<String, Object> metadata) {
    super(Compression.UNCOMPRESSED, MimeTypes.BINARY);
    this.recordClass = recordClass;
    this.codec = new PatchedSerializableAvroCodecFactory(codec);
    this.metadata = metadata;
  }

  public static <V extends Message> ProtobufFileOperation<V> of(
      Class<V> recordClass, CodecFactory codec, Map<String, Object> metadata) {
    Message m;
    return new ProtobufFileOperation<>(recordClass, codec, metadata);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("codecFactory", codec.getCodec().getClass()));
    builder.add(DisplayData.item("recordClass", recordClass));
  }

  @Override
  protected Reader<ValueT> createReader() {
    return new ProtobufReader<>(recordClass);
  }

  @Override
  protected FileIO.Sink<ValueT> createSink() {
    final AvroIO.Sink<ValueT> sink = AvroIO.sinkViaGenericRecords(
        avroBytesSchema,
        new AvroIO.RecordFormatter<ValueT>() {
          @Override
          public GenericRecord formatRecord(ValueT element, Schema schema) {
            return new GenericRecordBuilder(schema)
                .set("bytes", ByteBuffer.wrap(element.toByteArray()))
                .build();
          }
        }
    ).withCodec(codec.getCodec());

    if (metadata != null) {
      return sink.withMetadata(metadata);
    }
    return sink;
  }

  @Override
  public Coder<ValueT> getCoder() {
    return ProtoCoder.of(recordClass);
  }

  private final static Schema avroBytesSchema = SchemaBuilder.record("AvroBytesRecord")
      .fields()
      .requiredBytes("bytes")
      .endRecord();

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class ProtobufReader<ValueT extends Message>
      extends FileOperations.Reader<ValueT> {
    private Class<ValueT> recordClass;
    private transient DataFileStream<GenericRecord> reader;
    private transient Method parseFrom;

    ProtobufReader(Class<ValueT> recordClass) {
      this.recordClass = recordClass;
    }

    @Override
    public void prepareRead(ReadableByteChannel channel) throws IOException {

      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroBytesSchema);
      reader = new DataFileStream<>(Channels.newInputStream(channel), datumReader);
      try {
        parseFrom = recordClass.getMethod("parseFrom", ByteBuffer.class);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("Failed to prepare read for " + recordClass);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public ValueT readNext() throws IOException, NoSuchElementException {
      ByteBuffer bb = (ByteBuffer) reader.next().get("bytes");
      try {
        return (ValueT) parseFrom.invoke(null, bb);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(("Failed to parse bytes for " + recordClass));
      }
    }

    @Override
    public boolean hasNextElement() throws IOException {
      return reader.hasNext();
    }

    @Override
    public void finishRead() throws IOException {
      reader.close();
    }
  }
}
