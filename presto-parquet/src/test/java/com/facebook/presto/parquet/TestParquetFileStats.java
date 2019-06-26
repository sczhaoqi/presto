/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.createTempDirectory;
import static java.util.Arrays.asList;
import static org.apache.parquet.column.Encoding.DELTA_BYTE_ARRAY;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestParquetFileStats
{
    @Test
    public void testParquetStats()
    {
        // may assert fail when this stats was created by parquet prior to (parquet-1.10.0) parquet-1025
        testParquetStats(ImmutableList.of("中文", "日本語", "English"));
        testParquetStats(ImmutableList.of("中文", "日本語", "English", ""));
    }

    private void testParquetStats(List<String> strings)
    {
        PrimitiveType type = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test_binary_utf8");
        // mock parquet stats generate
        BinaryStatistics binaryStatistics = (BinaryStatistics) BinaryStatistics.createStats(type);
        for (String testString : strings) {
            binaryStatistics.updateStats(Binary.fromString(testString));
        }
        Binary maxBinary = binaryStatistics.genericGetMax();
        Binary minBinary = binaryStatistics.genericGetMin();

        for (String testString : strings) {
            assertTrue(minBinary.compareTo(Binary.fromString(testString)) <= 0, String.format("Binary min: %s,str: %s", minBinary.toStringUsingUTF8(), testString));
            assertTrue(maxBinary.compareTo(Binary.fromString(testString)) >= 0, String.format("Binary max: %s,str: %s", maxBinary.toStringUsingUTF8(), testString));
        }
        String max = maxBinary.toStringUsingUTF8();
        String min = minBinary.toStringUsingUTF8();
        // thrown assert-error when using parquet-mr prior 1.10.0 (PARQUET-1025),
        // effects TupleDomainParquetPredicate.getDomain
        // means
        //    not-contains-empty-string in parquet-blocks the condition minSlice.compareTo(maxSlice) always true
        //    contains-empty-string be false, but only accept empty-string and min-string will filter more blocks than expected
        assertTrue(Slices.utf8Slice(max).compareTo(Slices.utf8Slice(min)) >= 0, "Binary max not gather than min");
        // calculate min,max value for slice
        Slice maxSlice = null;
        Slice minSlice = null;
        for (String testString : strings) {
            Slice slice = Slices.utf8Slice(testString);
            if (maxSlice == null || maxSlice.compareTo(slice) < 0) {
                maxSlice = slice;
            }
            if (minSlice == null || minSlice.compareTo(slice) > 0) {
                minSlice = slice;
            }
        }
        // thrown assert-error when using parquet-mr prior 1.10.0 (PARQUET-1025)
        assertEquals(minSlice, Slices.utf8Slice(min), "Min-binary and Min-slice mismatch");
        assertEquals(maxSlice, Slices.utf8Slice(max), "Max-binary and Max-slice mismatch");
    }

    @Test
    public void testParquetFile()
            throws Exception
    {
        testParquetFile(ImmutableList.of("中文", "日本語", "English"));
        testParquetFile(ImmutableList.of("中文", "日本語", "English", ""));
    }

    private void testParquetFile(List<String> strings)
            throws Exception
    {
        Configuration conf = new Configuration();
        Path rootDirectory = tempDirectory();
        MessageType schema = parseMessageType(
                "message test { "
                        + "required int32 int32_field; "
                        + "required binary binary_field; "
                        + "required fixed_len_byte_array(3) flba_field; "
                        + "} ");
        GroupWriteSupport.setSchema(schema, conf);
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
        Map<String, Encoding> expected = ImmutableMap.of(
                "10-" + PARQUET_1_0, PLAIN_DICTIONARY,
                "1000-" + PARQUET_1_0, PLAIN,
                "10-" + PARQUET_2_0, RLE_DICTIONARY,
                "1000-" + PARQUET_2_0, DELTA_BYTE_ARRAY);
        for (int modulo : asList(10, 1000)) {
            ImmutableList.Builder<Integer> intValuesBuilder = ImmutableList.builder();
            ImmutableList.Builder<String> stringValuesBuilder = ImmutableList.builder();
            for (int i = 0; i < strings.size(); i++) {
                for (int j = 0; j < 1000; j++) {
                    intValuesBuilder.add(i);
                    stringValuesBuilder.add(strings.get(i) + j % modulo);
                }
            }

            List<Integer> intValues = intValuesBuilder.build();
            List<String> stringValues = stringValuesBuilder.build();

            for (ParquetProperties.WriterVersion version : ParquetProperties.WriterVersion.values()) {
                Path file = new Path(rootDirectory, version.name() + "_" + modulo);
                writeValues(file, version, schema, conf, intValues, stringValues);
                assertFileContents(file, conf, intValues, stringValues);
                ParquetMetadata footer = readFooter(conf, file, NO_FILTER);
                for (BlockMetaData blockMetaData : footer.getBlocks()) {
                    for (ColumnChunkMetaData column : blockMetaData.getColumns()) {
                        if (column.getPath().toDotString().equals("binary_field")) {
                            String key = modulo + "-" + version;
                            Encoding expectedEncoding = expected.get(key);
                            assertTrue(
                                    column.getEncodings().contains(expectedEncoding),
                                    key + ":" + column.getEncodings() + " should contain " + expectedEncoding);
                        }
                    }
                }
                assertFalse(ParquetCorruptStatisticsUtils.shouldIgnoreBinaryStatisticsForVarchars(footer.getFileMetaData()), "Only parquet-version after PARQUET-1025 can be entirely trustworthy");
            }
        }
    }

    private void writeValues(Path file, ParquetProperties.WriterVersion version, MessageType schema, Configuration conf, List<Integer> intValues, List<String> stringValues)
            throws IOException
    {
        ParquetWriter<Group> writer = new ParquetWriter<>(
                file,
                new GroupWriteSupport(),
                UNCOMPRESSED, 1024, 1024, 512, true, false, version, conf);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        for (int i = 0; i < intValues.size(); i++) {
            writer.write(factory.newGroup()
                    .append("int32_field", intValues.get(i))
                    .append("binary_field", stringValues.get(i))
                    .append("flba_field", "foo"));
        }
        writer.close();
    }

    private void assertFileContents(Path file, Configuration conf, List<Integer> intValues, List<String> stringValues)
            throws IOException
    {
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
        for (int i = 0; i < intValues.size(); i++) {
            Group group = reader.read();
            assertEquals(intValues.get(i).intValue(), group.getInteger("int32_field", 0));
            assertEquals(stringValues.get(i), group.getBinary("binary_field", 0).toStringUsingUTF8());
        }
        reader.close();
    }

    private static Path tempDirectory()
    {
        try {
            return new Path(createTempDirectory("ParquetStats").toString());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
