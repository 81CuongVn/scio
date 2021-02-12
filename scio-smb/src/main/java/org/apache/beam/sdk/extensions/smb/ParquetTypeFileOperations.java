/*
 * Copyright 2021 Spotify AB.
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

import magnolify.parquet.ParquetType;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;

/**
 * {@link org.apache.beam.sdk.extensions.smb.FileOperations} implementation for Parquet files with
 * Scala types.
 */
public class ParquetTypeFileOperations<ValueT> extends FileOperations<ValueT> {
  static final CompressionCodecName DEFAULT_COMPRESSION = CompressionCodecName.GZIP;

  private final ParquetType<ValueT> parquetType;
  private final Coder<ValueT> coder;
  private final CompressionCodecName compression;
  private final SerializableConfiguration conf;
  private final FilterPredicate predicate;

  private ParquetTypeFileOperations(
      ParquetType<ValueT> parquetType,
      Coder<ValueT> coder,
      CompressionCodecName compression,
      Configuration conf,
      FilterPredicate predicate) {
    super(Compression.UNCOMPRESSED, MimeTypes.BINARY);
    this.parquetType = parquetType;
    this.coder = coder;
    this.compression = compression;
    this.conf = new SerializableConfiguration(conf);
    this.predicate = predicate;
  }

  public static <V> ParquetTypeFileOperations<V> of(
      ParquetType<V> parquetType, Coder<V> coder) {
    return of(parquetType, coder, DEFAULT_COMPRESSION);
  }

  public static <V> ParquetTypeFileOperations<V> of(
      ParquetType<V> parquetType, Coder<V> coder, CompressionCodecName compression) {
    return of(parquetType, coder, compression, new Configuration());
  }

  public static <V> ParquetTypeFileOperations<V> of(
      ParquetType<V> parquetType,
      Coder<V> coder,
      CompressionCodecName compression,
      Configuration conf) {
    return new ParquetTypeFileOperations<>(
        parquetType, coder, compression, conf, null);
  }

  public static <V> ParquetTypeFileOperations<V> of(
      ParquetType<V> parquetType, Coder<V> coder, FilterPredicate predicate) {
    return new ParquetTypeFileOperations<>(
        parquetType, coder, DEFAULT_COMPRESSION, new Configuration(), predicate);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("compressionCodecName", compression.name()));
    builder.add(DisplayData.item("schema", parquetType.schema().getName()));
  }

  @Override
  protected Reader<ValueT> createReader() {
    return new ParquetTypeReader<>(parquetType, conf, predicate);
  }

  @Override
  protected FileIO.Sink<ValueT> createSink() {
    return new ParquetTypeSink<>(parquetType, compression, conf);
  }

  @Override
  public Coder<ValueT> getCoder() {
    return coder;
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class ParquetTypeReader<ValueT> extends FileOperations.Reader<ValueT> {
    private final ParquetType<ValueT> parquetType;
    private final SerializableConfiguration conf;
    private final FilterPredicate predicate;
    private transient ParquetReader<ValueT> reader;
    private transient ValueT current;

    private ParquetTypeReader(
        ParquetType<ValueT> parquetType,
        SerializableConfiguration conf,
        FilterPredicate predicate) {
      this.parquetType = parquetType;
      this.conf = conf;
      this.predicate = predicate;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareRead(ReadableByteChannel channel) throws IOException {
      ParquetReader.Builder<ValueT> builder =
          (ParquetReader.Builder<ValueT>)
          parquetType.readBuilder(new ParquetInputFile(channel)).withConf(conf.get());
      if (predicate != null) {
        builder = builder.withFilter(FilterCompat.get(predicate));
      }
      reader = builder.build();
      current = reader.read();
    }

    @Override
    public ValueT readNext() throws IOException, NoSuchElementException {
      ValueT r = current;
      current = reader.read();
      return r;
    }

    @Override
    public boolean hasNextElement() throws IOException {
      return current != null;
    }

    @Override
    public void finishRead() throws IOException {
      reader.close();
    }
  }

  ////////////////////////////////////////
  // Sink
  ////////////////////////////////////////

  private static class ParquetTypeSink<ValueT> implements FileIO.Sink<ValueT> {
    private final ParquetType<ValueT> parquetType;
    private final CompressionCodecName compression;
    private final SerializableConfiguration conf;
    private transient ParquetWriter<ValueT> writer;

    private ParquetTypeSink(
        ParquetType<ValueT> parquetType,
        CompressionCodecName compression,
        SerializableConfiguration conf) {
      this.parquetType = parquetType;
      this.compression = compression;
      this.conf = conf;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open(WritableByteChannel channel) throws IOException {
      // https://github.com/apache/parquet-mr/tree/master/parquet-hadoop#class-parquetoutputformat
      int rowGroupSize =
          conf.get().getInt(ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE);
      writer = (ParquetWriter<ValueT>) parquetType
          .writeBuilder(new ParquetOutputFile(channel))
          .withCompressionCodec(compression)
          .withConf(conf.get())
          .withRowGroupSize(rowGroupSize)
          .build();
    }

    @Override
    public void write(ValueT element) throws IOException {
      writer.write(element);
    }

    @Override
    public void flush() throws IOException {
      writer.close();
    }
  }
}
