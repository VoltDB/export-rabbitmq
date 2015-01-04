/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltDB;
import org.voltdb.export.AdvertisedDataSource;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class TestRabbitMQExportClient extends ExportClientTestBase {
    @Before
    public void setup()
    {
        super.setup();
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
