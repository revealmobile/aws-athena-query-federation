/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.connectors.athena.example;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import com.revealmobile.geohashtrie.GeohashTrie;
import com.revealmobile.geohashtrie.GeohashUtils;

import com.github.davidmoten.geo.GeoHash;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example RecordHandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with actual rows level data from your source. Athena
 * will call readWithConstraint(...) on this class for each 'Split' you generated in ExampleMetadataHandler.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class ExampleRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleRecordHandler.class);

    private AmazonS3 amazonS3;

    public ExampleRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient());
    }

    @VisibleForTesting
    protected ExampleRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena amazonAthena)
    {
        super(amazonS3, secretsManager, amazonAthena, ExampleMetadataHandler.SOURCE_TYPE);
        this.amazonS3 = amazonS3;
    }

    static InputStream openStream(final String src, final InputStream inputStream) throws IOException {
        if (src.endsWith(".gz")) {
            return new GZIPInputStream(inputStream, 65535);
        } else {
            return new BufferedInputStream(inputStream, 65535);
        }
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller A BlockSpiller that should be used to write the row data associated with this Split.
     * The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest Details of the read request, including:
     * 1. The Split
     * 2. The Catalog, Database, and Table the read request is for.
     * 3. The filtering predicate (if any)
     * 4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws IOException
     * @note Avoid writing >10 rows per-call to BlockSpiller.writeRow(...) because this will limit the BlockSpiller's
     * ability to control Block size. The resulting increase in Block size may cause failures and reduced performance.
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws IOException
    {
        logger.info("readWithConstraint: enter - " + recordsRequest.getSplit());

        final ValueSet triePath = recordsRequest.getConstraints().getSummary().get("geohash_trie");
        final List<GeohashRange> ranges = new ArrayList<>();
        if (triePath == null) {
            logger.warn("no geohash range or Geohash Trie path set via (geohash_trie = 's3://...'). Aborting");

            return;
        }

        GeohashTrie trie = null;
        if (triePath.isSingleValue()) {
            if (triePath.getSingleValue().toString().startsWith("s3://")) {
                try {
                    final java.net.URI objectURI = new java.net.URI(triePath.getSingleValue().toString());
                    try (S3Object obj = amazonS3.getObject(objectURI.getHost(), objectURI.getPath().substring(1));
                        DataInputStream dataInput = new DataInputStream(openStream(obj.getKey(), obj.getObjectContent()))) {
                        GeohashTrie.readHeader(dataInput);
                        trie = GeohashTrie.read(dataInput);
                        for (long[] bound : trie.getBoundsAt(2)) {
                            ranges.add(new GeohashRange(GeohashUtils.fromLongToString(bound[0]), GeohashRange.rightPad(GeohashUtils.fromLongToString(bound[1]), "z", 10)));
                        }
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } else if (triePath.getSingleValue().toString().toUpperCase().startsWith("POLYGON(")) {
                trie = ExampleMetadataHandler.convertWKT(triePath.getSingleValue().toString().toUpperCase());
                for (long[] bound : trie.getBoundsAt(2)) {
                    ranges.add(new GeohashRange(GeohashUtils.fromLongToString(bound[0]), GeohashRange.rightPad(GeohashUtils.fromLongToString(bound[1]), "z", 10)));
                }
            }
        } else {
            for (Range r : triePath.getRanges().getOrderedRanges()) {
                ranges.add(new GeohashRange(r.getLow().getValue().toString(), GeohashRange.rightPad(r.getHigh().getValue().toString(), "z", 10)));
            }
        }

        Split split = recordsRequest.getSplit();

        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());

        builder.withExtractor("d", (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = split.getProperty("d");
        });

        builder.withExtractor("type_id", (IntExtractor) (Object context, NullableIntHolder value) -> {
            value.isSet = 1;
            value.value = split.getPropertyAsInt("type_id");
        });

        builder.withExtractor("advertiser_id", (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = ((SimpleGroup) context).getString("advertiser_id", 0);
        });

        builder.withExtractor("os", (IntExtractor) (Object context, NullableIntHolder value) -> {
            value.isSet = 1;
            value.value = ((SimpleGroup) context).getInteger("os", 0);
        });

        builder.withExtractor("lat", (Float8Extractor) (Object context, NullableFloat8Holder value) -> {
            value.isSet = 1;
            value.value = GeoHash.decodeHash(((SimpleGroup) context).getString("geohash", 0)).getLat();
        });

        builder.withExtractor("lon", (Float8Extractor) (Object context, NullableFloat8Holder value) -> {
            value.isSet = 1;
            value.value = GeoHash.decodeHash(((SimpleGroup) context).getString("geohash", 0)).getLon();
        });

        builder.withExtractor("geohash", (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = ((SimpleGroup) context).getString("geohash", 0);
        });

        //unless we find out that we can mark a constraint as satisfied by partition pruning
        //we need to trick the final query engine by assigning an input constraint (or part of one) as an output column
        builder.withExtractor("geohash_trie", (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
            value.isSet = 1;
            value.value = triePath.isSingleValue() ? triePath.getSingleValue().toString() : triePath.getRanges().getOrderedRanges().get(0).getLow().getValue().toString();
        });

        /**
         * TODO: The account_id field is a sensitive field, so we'd like to mask it to the last 4 before
         *  returning it to Athena. Note that this will mean you can only filter (where/having)
         *  on the masked value from Athena.
         *
         builder.withExtractor("account_id", (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
             value.isSet = 1;
             String accountId = ((String[]) context)[3];
             value.value = accountId.length() > 4 ? accountId.substring(accountId.length() - 4) : accountId;
         });
         */

        /**
         * TODO: Write data for our transaction STRUCT:
         * For complex types like List and Struct, we can build a Map to conveniently set nested values
         *
         builder.withFieldWriterFactory("transaction",
                (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                    (Object context, int rowNum) -> {
                         Map<String, Object> eventMap = new HashMap<>();
                         eventMap.put("id", Integer.parseInt(((String[])context)[4]));
                         eventMap.put("completed", Boolean.parseBoolean(((String[])context)[5]));
                         BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, eventMap);
                         return true;    //we don't yet support predicate pushdown on complex types
         });
         */

        //Used some basic code-gen to optimize how we generate response data.
        GeneratedRowWriter rowWriter = builder.build();

        try {
            final Map<String, SimpleGroup> uniques = readFiles(ranges, trie, split.getProperty("paths").split(","));

            logger.debug("writing " + uniques.size() + " rows");
            //TODO rollup distinct values

            //We read the transaction data line by line from our S3 object.
            for (SimpleGroup values : uniques.values()) {
                //We use the provided BlockSpiller to write our row data into the response. This utility is provided by
                //the Amazon Athena Query Federation SDK and automatically handles breaking the data into reasonably sized
                //chunks, encrypting it, and spilling to S3 if we've enabled these features.
                spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, values) ? 1 : 0);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static final ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(System.getProperty("concurrent.readers", "5")));

    public static Map<String, SimpleGroup> readFiles(final Iterable<GeohashRange> ranges, final GeohashTrie trie, final String... paths) throws Exception {
        // TODO uniques after stream join
        final HashMap<String, SimpleGroup> uniques = new HashMap<>();

        final BinaryColumn ghCol = FilterApi.binaryColumn("geohash");
        FilterPredicate fp = null;
        for (GeohashRange range : ranges) {
            final FilterPredicate subfilter = FilterApi.and(FilterApi.gtEq(ghCol, Binary.fromString(range.getLower())), FilterApi.ltEq(ghCol, Binary.fromString(range.getUpper())));
            fp = fp == null ? subfilter : FilterApi.or(fp, subfilter);
        }
        final FilterCompat.Filter filter = FilterCompat.get(fp);

        final ParquetReadOptions readOptions = ParquetReadOptions.builder().withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build();

        final int advertiserIdFieldIdx = 0;
        final int osFieldIdx = 1;
        final int geohashFieldIdx = 2;

        final AtomicInteger recordCount = new AtomicInteger();
        final ArrayList<Callable<Void>> tasks = new ArrayList<>(paths.length);

        final Configuration conf = new Configuration();
        for (String path : paths) {
            final Path parquetFilePath = new Path(path.replaceAll("s3://","s3a://"));
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    final InputFile in = HadoopInputFile.fromPath(parquetFilePath, conf);

                    final ParquetFileReader r = ParquetFileReader.open(in, readOptions);

                    final MessageType fileSchema = r.getFileMetaData().getSchema();
                    final MessageType projection = new MessageType(fileSchema.getName(), Arrays.asList(
                            fileSchema.getFields().get(fileSchema.getFieldIndex("advertiser_id")),
                            fileSchema.getFields().get(fileSchema.getFieldIndex("os")),
                            fileSchema.getFields().get(fileSchema.getFieldIndex("geohash"))
                    ));
                    PageReadStore pageReadStore = r.readNextRowGroup();

                    while (pageReadStore != null) {
                        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projection, fileSchema);
                        RecordReader recordReader = columnIO.getRecordReader(pageReadStore, new GroupRecordConverter(projection), filter);

                        for (int i = 0; i < pageReadStore.getRowCount(); i++) {
                            SimpleGroup record = (SimpleGroup) recordReader.read();
                            if (!recordReader.shouldSkipCurrentRecord()) {
                                final String uniqueKey = record.getString(advertiserIdFieldIdx, 0) + record.getInteger(osFieldIdx, 0);
                                synchronized (uniques) {
                                    if (!uniques.containsKey(uniqueKey)) {
                                        if (trie == null || trie.contains(GeohashUtils.fromStringToLong(record.getString(geohashFieldIdx, 0)))) {
                                            uniques.put(uniqueKey, record);
                                        }
                                    }
                                }
                            }
                            if (recordCount.incrementAndGet() % 10000 == 0) {
                                logger.debug("Processed " + recordCount.get() + " records, resulting in " + uniques.size() + " distinct values");
                            }
                        }
                        pageReadStore = r.readNextRowGroup();
                    }

                    r.close();
                    return null;
                }
            });
        }

        executor.invokeAll(tasks);

        return uniques;
    }

    public static void main(String[] args) {
        if (args.length >= 2) {
            ArrayList<GeohashRange> ranges = new ArrayList<>();
            for (int i=1; i<args.length; i++) {
                ranges.add(GeohashRange.parse(args[i]));
            }
            try {
                final Map<String, SimpleGroup> uniques = readFiles(ranges, null, args[0]);
                for (Map.Entry e : uniques.entrySet()) {
                    System.out.println(e.getKey() + ": " + e.getValue());
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
