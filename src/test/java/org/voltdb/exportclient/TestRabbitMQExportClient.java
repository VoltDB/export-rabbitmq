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
import org.junit.Test;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.export.AdvertisedDataSource;

import java.util.ArrayList;
import java.util.Properties;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class TestRabbitMQExportClient {
    @Test
    public void testConfigValidation() throws Exception
    {
        final RabbitMQExportClient dut = new RabbitMQExportClient();
        Properties emptyConfig = new Properties();
        try {
            dut.configure(emptyConfig);
            fail("broker.host should be required");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("broker.host"));
        }
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
        assertEquals("", dut.m_exchangeName);
        assertEquals(null, dut.m_routingKey);
        assertFalse(dut.m_skipInternal);
        assertEquals(MessageProperties.PERSISTENT_TEXT_PLAIN, dut.m_channelProperties);
        assertEquals(ExportDecoderBase.BinaryEncoding.HEX, dut.m_binaryEncoding);
        assertEquals(TimeZone.getTimeZone(VoltDB.GMT_TIMEZONE.getID()), dut.m_ODBCDateformat.get().getTimeZone());
    }

    @Test
    public void testEffectiveRoutingKey() throws Exception
    {
        final RabbitMQExportClient dut = new RabbitMQExportClient();
        final Properties config = new Properties();
        config.setProperty("broker.host", "localhost");
        dut.configure(config);

        ArrayList<String> colNames = Lists.newArrayList("col1", "col2");
        ArrayList<VoltType> colTypes = Lists.newArrayList(VoltType.BIGINT, VoltType.FLOAT);
        ArrayList<Integer> lens = Lists.newArrayList(8, 8);
        final RabbitMQExportClient.RabbitExportDecoder decoder =
                (RabbitMQExportClient.RabbitExportDecoder) dut.constructExportDecoder(
                        new AdvertisedDataSource(1, "TableSig", "TableName", "col1",
                                System.currentTimeMillis(), 0, colNames, colTypes, lens));
        assertEquals("TableName_1", decoder.m_effectiveRoutingKey);
    }
}
