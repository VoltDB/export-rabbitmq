/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.exportclient;

import com.google_voltpatches.common.collect.Lists;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.export.AdvertisedDataSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class TestRabbitMQExportClient {
    static final String[] COLUMN_NAMES = {"tid", "ts", "sq", "pid", "site", "op",
            "tinyint", "smallint", "integer", "bigint", "float", "timestamp", "string", "decimal"};

    static final VoltType[] COLUMN_TYPES
            = {VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT,
            VoltType.TINYINT, VoltType.SMALLINT, VoltType.INTEGER,
            VoltType.BIGINT, VoltType.FLOAT, VoltType.TIMESTAMP,VoltType.STRING, VoltType.DECIMAL};

    static final Integer COLUMN_LENGTHS[] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0
    };

    static VoltTable vtable = new VoltTable(
            new VoltTable.ColumnInfo("VOLT_TRANSACTION_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_EXPORT_TIMESTAMP", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_EXPORT_SEQUENCE_NUMBER", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_PARTITION_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_TRANSACTION_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_SITE_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("tinyint", VoltType.TINYINT),
            new VoltTable.ColumnInfo("smallint", VoltType.SMALLINT),
            new VoltTable.ColumnInfo("integer", VoltType.INTEGER),
            new VoltTable.ColumnInfo("bigint", VoltType.BIGINT),
            new VoltTable.ColumnInfo("float", VoltType.FLOAT),
            new VoltTable.ColumnInfo("timestamp", VoltType.TIMESTAMP),
            new VoltTable.ColumnInfo("string", VoltType.STRING),
            new VoltTable.ColumnInfo("decimal", VoltType.DECIMAL)
    );

    static AdvertisedDataSource constructTestSource(boolean replicated, int partition)
    {
        ArrayList<String> col_names = new ArrayList<String>();
        ArrayList<VoltType> col_types = new ArrayList<VoltType>();
        for (int i = 0; i < COLUMN_TYPES.length; i++)
        {
            col_names.add(COLUMN_NAMES[i]);
            col_types.add(COLUMN_TYPES[i]);
        }
        String partCol = replicated ? null : "smallint";
        //clear the table
        vtable.clearRowData();
        AdvertisedDataSource source = new AdvertisedDataSource(partition, "foo", "yankeelover",
                partCol, 0, 32, col_names, col_types, Arrays.asList(COLUMN_LENGTHS));
        return source;
    }

    @Before
    public void setup()
    {
        vtable.clearRowData();
    }

    @Test
    public void testConfigValidation() throws Exception
    {
        final RabbitMQExportClient dut = new RabbitMQExportClient();
        Properties emptyConfig = new Properties();
        try {
            dut.configure(emptyConfig);
            fail("broker.host or amqp.uri should be required");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("broker.host"));
        }

        // Set host
        Properties hostConfig = new Properties();
        hostConfig.setProperty("broker.host", "fakehost");
        dut.configure(hostConfig);

        // Set URI
        Properties uriConfig = new Properties();
        uriConfig.setProperty("amqp.uri", "amqp://volt:adhoc@fakehost:7000/myvhost");
        dut.configure(uriConfig);
    }

    /**
     * Make sure we use sane defaults for optional config options.
     */
    @Test
    public void testDefaultConfig() throws Exception
    {
        final RabbitMQExportClient dut = new RabbitMQExportClient();
        final Properties config = new Properties();
        config.setProperty("broker.host", "fakehost");
        dut.configure(config);
        assertEquals("fakehost", dut.m_connFactory.getHost());
        assertEquals(ConnectionFactory.DEFAULT_AMQP_PORT, dut.m_connFactory.getPort());
        assertEquals(ConnectionFactory.DEFAULT_USER, dut.m_connFactory.getUsername());
        assertEquals(ConnectionFactory.DEFAULT_PASS, dut.m_connFactory.getPassword());
        assertEquals(ConnectionFactory.DEFAULT_VHOST, dut.m_connFactory.getVirtualHost());
        assertEquals("", dut.m_exchangeName);
        assertTrue(dut.m_routingKeyColumns.isEmpty());
        assertFalse(dut.m_skipInternal);
        assertEquals(MessageProperties.PERSISTENT_TEXT_PLAIN, dut.m_channelProperties);
        assertEquals(ExportDecoderBase.BinaryEncoding.HEX, dut.m_binaryEncoding);
        assertEquals(TimeZone.getTimeZone(VoltDB.GMT_TIMEZONE.getID()), dut.m_ODBCDateformat.get().getTimeZone());
    }

    @Test
    public void testRoutingKeySuffixParsing() throws Exception
    {
        final RabbitMQExportClient dut = new RabbitMQExportClient();
        final Properties config = new Properties();
        config.setProperty("broker.host", "fakehost");
        config.setProperty("routing.key.suffix", "table1.col1,table2.col2,table3.col3, table4.col4,");
        dut.configure(config);
        assertEquals("col1", dut.m_routingKeyColumns.get("table1"));
        assertEquals("col2", dut.m_routingKeyColumns.get("table2"));
        assertEquals("col3", dut.m_routingKeyColumns.get("table3"));
        assertEquals("col4", dut.m_routingKeyColumns.get("table4"));
    }

    @Test
    public void testPartitionColumnEffectiveRoutingKey() throws Exception
    {
        // Use partitioning column
        verifyRoutingKey(/* replicated */ false,
                         /* colName */ null,
                         /* expectedRoutingKey */ "yankeelover.2");
        // Use non-partitioning column
        verifyRoutingKey(/* replicated */ false,
                         /* colName */ "yankeelover.bigint",
                         /* expectedRoutingKey */ "yankeelover.4");

        // Replicated table, no routing suffix by default
        verifyRoutingKey(/* replicated */ true,
                         /* colName */ null,
                         /* expectedRoutingKey */ "yankeelover");
        // Replicated table, specify routing suffix
        verifyRoutingKey(/* replicated */ true,
                         /* colName */ "yankeelover.integer",
                         /* expectedRoutingKey */ "yankeelover.3");

        // Unknown column, no routing suffix
        verifyRoutingKey(/* replicated */ false,
                         /* colName */ "yankeelover.doesntexist",
                         /* expectedRoutingKey */ "yankeelover");
    }

    private void verifyRoutingKey(boolean replicated,
                                  String colName,
                                  String expectedRoutingKey)
        throws Exception
    {
        final RabbitMQExportClient dut = new RabbitMQExportClient();
        final Properties config = new Properties();
        config.setProperty("broker.host", "localhost");
        if (colName != null) {
            config.setProperty("routing.key.suffix", colName);
        }
        dut.configure(config);

        final AdvertisedDataSource source = constructTestSource(replicated, 0);
        final RabbitMQExportClient.RabbitExportDecoder decoder =
                (RabbitMQExportClient.RabbitExportDecoder) dut.constructExportDecoder(source);

        long l = System.currentTimeMillis();
        vtable.addRow(l, l, l, 0, l, l, (byte) 1,
                /* partitioning column */ (short) 2,
                3, 4, 5.5, 6, "xx", new BigDecimal(88));
        vtable.advanceRow();
        byte[] rowBytes = ExportEncoder.encodeRow(vtable);
        final ExportDecoderBase.ExportRowData rowData = decoder.decodeRow(rowBytes);
        assertEquals(expectedRoutingKey, decoder.getEffectiveRoutingKey(rowData));
    }
}
