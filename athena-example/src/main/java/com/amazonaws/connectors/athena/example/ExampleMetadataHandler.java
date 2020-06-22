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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
//DO NOT REMOVE - this will not be _unused_ when customers go through the tutorial and uncomment
//the TODOs
import org.apache.arrow.vector.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.revealmobile.geohashtrie.GeohashTrie;
import com.revealmobile.geohashtrie.GeohashUtils;
import com.revealmobile.geohashtrie.ShapeCoverage;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.ogc.OGCGeometry;

/**
 * This class is part of an tutorial that will walk you through how to build a connector for your
 * custom data source. The README for this module (athena-example) will guide you through preparing
 * your development environment, modifying this example Metadatahandler, building, deploying, and then
 * using your new source in an Athena query.
 * <p>
 * More specifically, this class is responsible for providing Athena with metadata about the schemas (aka databases),
 * tables, and table partitions that your source contains. Lastly, this class tells Athena how to split up reads against
 * this source. This gives you control over the level of performance and parallelism your source can support.
 * <p>
 * For more examples, please see the other connectors in this repository (e.g. athena-cloudwatch, athena-docdb, etc...)
 */
public class ExampleMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleMetadataHandler.class);

    private static final String SCHEMA = "primary";

    private static final String EVENT_TABLE = "event";

    /**
     * used to aid in debugging. Athena will use this name in conjunction with your catalog id
     * to correlate relevant query errors.
     */
    static final String SOURCE_TYPE = "RevealGeohashTrie";

    private static final int PREFERRED_CONCURRENCY = 10;

    public static final String BASE32_ALPHABET = "0123456789bcdefghjkmnpqrstuvwxyz";

    private static final Pattern PATH_PATTERN = Pattern.compile(".*geohash-range_(["+BASE32_ALPHABET+"]+)-(["+BASE32_ALPHABET+"]+).*");

    private final AmazonS3 amazonS3;

    public ExampleMetadataHandler()
    {
        super(SOURCE_TYPE);

        amazonS3 = AmazonS3ClientBuilder.defaultClient();
    }

    @VisibleForTesting
    protected ExampleMetadataHandler(EncryptionKeyFactory keyFactory,
            AWSSecretsManager awsSecretsManager,
            AmazonAthena athena,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);

        amazonS3 = AmazonS3ClientBuilder.defaultClient();
    }

    /**
     * Used to get the list of schemas (aka databases) that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: enter - " + request);

        return new ListSchemasResponse(request.getCatalogName(), Collections.singleton(SCHEMA));
    }

    /**
     * Used to get the list of tables that this source contains.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables: enter - " + request);

        return new ListTablesResponse(request.getCatalogName(), Arrays.asList(new TableName(request.getSchemaName(), EVENT_TABLE)));
    }

    /**
     * Used to get definition (field names, types, descriptions, etc...) of a Table.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set<String> of partition column names (or empty if the table isn't partitioned).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        logger.info("doGetTable: enter - " + request);

        Set<String> partitionColNames = new HashSet<>();

        partitionColNames.add("d");
        partitionColNames.add("type_id");

        SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();

        tableSchemaBuilder
        .addStringField("d")
        .addIntField("type_id")
        .addStringField("advertiser_id")
        .addIntField("os")
        .addFloat8Field("lat")
        .addFloat8Field("lon")
        .addStringField("geohash")
        .addStringField("geohash_trie") //special treatment for pruning files
        //Metadata who's name matches a column name
        //is interpreted as the description of that
        //column when you run "show tables" queries.
        .addMetadata("d", "The date that the event took place in.")
        .addMetadata("type_id", "The type of event.")
        .addMetadata("advertiser_id", "The device identifier")
        .addMetadata("os", "The type of device")
        .addMetadata("geohash", "The geohash32 representation of the lat,lon.")
        .addMetadata("geohash_trie", "A special field used for pruning files")
        //This metadata field is for our own use, Athena will ignore and pass along fields it doesn't expect.
        //we will use this later when we implement doGetTableLayout(...)
        .addMetadata("partitionCols", "d,type_id");

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                tableSchemaBuilder.build(),
                partitionColNames);
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @note Partitions are partially opaque to Amazon Athena in that it only understands your partition columns and
     * how to filter out partitions that do not meet the query's constraints. Any additional columns you add to the
     * partition data are ignored by Athena but passed on to calls on GetSplits.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        final Calendar start = Calendar.getInstance();
        start.add(Calendar.DATE, -1*start.get(Calendar.DATE));
        start.add(Calendar.YEAR, -2); //TODO make this configurable?
        final Calendar end = Calendar.getInstance();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        while (!start.after(end)) {
            final String d = sdf.format(start.getTime());
            for (int t=1; t<3; t++) {
                final int type_id = t;
                //logger.debug("checking partition " + d + ", " + type_id);
                blockWriter.writeRows((Block block, int row) -> {
                    boolean matched = true;
                    matched &= block.setValue("d", row, d);
                    matched &= block.setValue("type_id", row, type_id);
                    //If all fields matches then we wrote 1 row during this call so we return 1
                    return matched ? 1 : 0;
                });
            }
            start.add(Calendar.DATE, 1);
        }
    }

    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, andpartition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set<Split> which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     * @note A Split is a mostly opaque object to Amazon Athena. Amazon Athena will use the optional SpillLocation and
     * optional EncryptionKey for pipelined reads but all properties you set on the Split are passed to your read
     * function to help you perform the read.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        logger.info("doGetSplits: enter - " + request);

        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();
        //IMPORTANT: the RecordHandler implementation assumes one partition per split

        final ValueSet triePath = request.getConstraints().getSummary().get("geohash_trie");
        final List<GeohashRange> ranges = new ArrayList<>();
        if (triePath == null) {
            logger.warn("no geohash range or Geohash Trie path set via (geohash_trie = 's3://...'). Aborting");

            return new GetSplitsResponse(catalogName, splits);
        }
        if (triePath.isSingleValue()) {
            if (triePath.getSingleValue().toString().startsWith("s3://")) {
                try {
                    final java.net.URI objectURI = new java.net.URI(triePath.getSingleValue().toString());
                    try (S3Object obj = amazonS3.getObject(objectURI.getHost(), objectURI.getPath().substring(1));
                         DataInputStream dataInput = new DataInputStream(ExampleRecordHandler.openStream(obj.getKey(), obj.getObjectContent()))) {
                        GeohashTrie.readHeader(dataInput);
                        final GeohashTrie trie = GeohashTrie.read(dataInput);
                        for (long[] bound : trie.getBoundsAt(2)) {
                            ranges.add(new GeohashRange(GeohashUtils.fromLongToString(bound[0]), GeohashRange.rightPad(GeohashUtils.fromLongToString(bound[1]), "z", 10)));
                        }
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } else if (triePath.getSingleValue().toString().toUpperCase().startsWith("POLYGON(")) {
                final GeohashTrie trie = convertWKT(triePath.getSingleValue().toString().toUpperCase());
                for (long[] bound : trie.getBoundsAt(2)) {
                    ranges.add(new GeohashRange(GeohashUtils.fromLongToString(bound[0]), GeohashRange.rightPad(GeohashUtils.fromLongToString(bound[1]), "z", 10)));
                }
            }
            //TODO throw exception or get all ranges from all tries
        } else {
            for (Range r : triePath.getRanges().getOrderedRanges()) {
                ranges.add(new GeohashRange(r.getLow().getValue().toString(), GeohashRange.rightPad(r.getHigh().getValue().toString(), "z", 10)));
            }
        }

        for (GeohashRange range : ranges) {
            //logger.debug(range.getLower() + " - " + range.getUpper());
        }

        Block partitions = request.getPartitions();

        FieldReader d = partitions.getFieldReader("d");
        FieldReader typeId = partitions.getFieldReader("type_id");

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

        //goal: more files per split when total partitions is high,
        //also want at least 2 files in each split to mitigate startup time penalty
        final int maxSplitSize = Math.min(Math.max(5, partitions.getRowCount()), 10);
        final List<String> accepted = new ArrayList<>();

        //logger.debug("listing " + partitions.getRowCount() + " partitions");

        for (int i = 0; i < partitions.getRowCount(); i++) {
            //Set the readers to the partition row we are on
            d.setPosition(i);
            typeId.setPosition(i);
            final Calendar date = Calendar.getInstance();
            try {
                date.setTime(sdf.parse(d.readText().toString()));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            final String prefix = "parquet/event/year="+date.get(Calendar.YEAR)+"/month="+(date.get(Calendar.MONTH)+1)+"/day="+date.get(Calendar.DAY_OF_MONTH)+"/type_id="+typeId.readInteger();
            //logger.debug("listing " + prefix);

            ListObjectsV2Result result = amazonS3.listObjectsV2(
                    new ListObjectsV2Request()
                        .withBucketName("reveal-spark")
                        .withPrefix(prefix)
            );

            while (true) {
                logger.debug("testing " + result.getObjectSummaries().size() + " keys");
                for (S3ObjectSummary obj : result.getObjectSummaries()) {
                    final Matcher matcher = PATH_PATTERN.matcher(obj.getKey());
                    if (matcher.matches()) {
                        for (GeohashRange range : ranges) {
                            if (range.overlaps(matcher.group(1), matcher.group(2))) {
                                //logger.debug("accepting s3://" + obj.getBucketName() + "/" + obj.getKey() + " against " + range.getLower() + "-" + range.getUpper());
                                accepted.add("s3://" + obj.getBucketName() + "/" + obj.getKey());
                                break;
                            }
                        }
                    } else {
                        accepted.add("s3://" + obj.getBucketName() + "/" + obj.getKey());
                    }
                }
                if (result.getNextContinuationToken() == null) {
                    break;
                }
                result = amazonS3.listObjectsV2(
                        new ListObjectsV2Request()
                                .withContinuationToken(result.getNextContinuationToken())
                                .withBucketName("reveal-spark")
                                .withPrefix(prefix)
                );
            }

            for (int s = 0; s < accepted.size(); s += maxSplitSize) {
                splits.add(
                        Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                                .add("d", d.readText().toString())
                                .add("type_id", String.valueOf(typeId.readInteger()))
                                .add("paths", String.join(",", accepted.subList(s, Math.min(accepted.size(), s+maxSplitSize))))
                                .build()
                );
            }
        }

        logger.info("doGetSplits: exit - " + splits.size());
        return new GetSplitsResponse(catalogName, splits);
    }

    static GeohashTrie convertWKT(final String wkt) {
        final GeohashTrie trie = new GeohashTrie();
        final ShapeCoverage coverage = new ShapeCoverage();
        final Geometry geom = OGCGeometry.fromText(wkt).getEsriGeometry();
        final Envelope envel = new Envelope();
        geom.queryEnvelope(envel);
        coverage.cover(geom, envel, 9, trie, 0);
        return trie;
    }
}
