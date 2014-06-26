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

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.RateLimitedLogger;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.AMQP.BasicProperties;

import au.com.bytecode.opencsv_voltpatches.CSVWriter;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource;

public class RabbitMQExportClient extends ExportClientBase {

    private static final VoltLogger slogger = new VoltLogger("ExportClient");
    private static final RateLimitedLogger errlogger = new RateLimitedLogger(10 * 1000, slogger, Level.ERROR);

    // The following variables are all RabbitMQ config options.
    // RabbitMQ server hostname/IP.
    String m_brokerHost;
    // RabbitMQ server port number.
    int m_brokerPort;
    // RabbitMQ exchange name.
    String m_exchangeName;
    // The routing key to use for each message.
    String m_routingKey;

    boolean m_skipInternal;
    ExportDecoderBase.BinaryEncoding m_binaryEncoding;
    // use thread-local to avoid SimpleDateFormat thread-safety issues
    ThreadLocal<SimpleDateFormat> m_ODBCDateformat;

    BasicProperties m_channelProperties;

    @Override
    public void configure(Properties config) throws Exception {
        m_brokerHost = config.getProperty("broker.host");
        if (m_brokerHost == null) {
            throw new IllegalArgumentException("\"broker.host\" must not be null");
        }

        m_brokerPort = Integer.parseInt(config.getProperty("broker.port",
                String.valueOf(ConnectionFactory.DEFAULT_AMQP_PORT)));
        m_exchangeName = config.getProperty("exchange.name", "");
        m_routingKey =  config.getProperty("routing.key");
        m_skipInternal = Boolean.parseBoolean(config.getProperty("skipinternals", "false"));

        if (Boolean.parseBoolean(config.getProperty("queue.durable", "true"))) {
            m_channelProperties = MessageProperties.PERSISTENT_TEXT_PLAIN;
        }

        final TimeZone tz = TimeZone.getTimeZone(config.getProperty("timezone", VoltDB.GMT_TIMEZONE.getID()));

        m_binaryEncoding = ExportDecoderBase.BinaryEncoding.valueOf(
                config.getProperty("binaryencoding", "HEX").trim().toUpperCase());

        m_ODBCDateformat = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                SimpleDateFormat sdf = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
                sdf.setTimeZone(tz);
                return sdf;
            }
        };

        slogger.info("Configured RabbitMQ export client");
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new RabbitExportDecoder(source);
    }

    class RabbitExportDecoder extends ExportDecoderBase {
        final String m_effectiveRoutingKey;

        private ConnectionFactory m_connFactory;
        private Connection m_connection;
        private Channel m_channel;
        private final ListeningExecutorService m_es;

        RabbitExportDecoder(AdvertisedDataSource source) {
            super(source);
            slogger.info("Creating Rabbit export decoder for " + source.toString());
            if (m_routingKey == null) {
                m_effectiveRoutingKey = m_source.tableName + "_" + m_source.partitionId;
            } else {
                m_effectiveRoutingKey = m_routingKey;
            }

            m_connFactory = new ConnectionFactory();
            m_connFactory.setHost(m_brokerHost);
            m_connFactory.setPort(m_brokerPort);

            m_es = CoreUtils.getListeningSingleThreadExecutor(
                    "RabbitMQ Export decoder for partition " + source.partitionId
                    + " table " + source.tableName
                    + " generation " + source.m_generation, CoreUtils.SMALL_STACK_SIZE);
        }

        private Connection getConnection() throws IOException {
            if (m_connection != null) {
                return m_connection;
            }

            if (slogger.isDebugEnabled()) {
                slogger.debug(String.format("Connecting to RabbitMQ server %s on port %d",
                        m_brokerHost, m_brokerPort));
            }

            m_connection = m_connFactory.newConnection();

            return m_connection;
        }

        private Channel getChannel() throws IOException {
            if (m_channel != null) {
                return m_channel;
            }

            Connection cn = getConnection();
            m_channel = cn.createChannel();
            m_channel.confirmSelect();
            return m_channel;
        }

        private void close() {
            closeChannel();
            closeConnection();
        }

        private void closeChannel() {
            if (m_channel == null) {
                return;
            }
            try {
                m_channel.close();
            } catch (IOException e) {
                slogger.warn("RabbitMQ export client failed to close channel.", e);
            } finally {
                m_channel = null;
            }
        }

        private void closeConnection() {
            if (m_connection == null) {
                return;
            }
            try {
                m_connection.close();
            } catch (IOException e) {
                slogger.warn("RabbitMQ export client failed to close connection.", e);
            } finally {
                m_connection = null;
            }
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData)
                throws RestartBlockException {
            StringWriter stringer = new StringWriter();
            CSVWriter csv = new CSVWriter(stringer);
            try {
                final Object[] row = decodeRow(rowData);
                if (!writeRow(row, csv, m_skipInternal, m_binaryEncoding, m_ODBCDateformat.get())) {
                    return false;
                }
                csv.flush();

                String message = stringer.toString();

                if (slogger.isTraceEnabled()) {
                    slogger.trace(String.format("Publishing to exchange %s using routing key %s",
                            m_exchangeName, m_effectiveRoutingKey));
                }

                getChannel().basicPublish(m_exchangeName, m_effectiveRoutingKey, m_channelProperties, message.getBytes());
            } catch(Exception e) {
                errlogger.log("Failed to send row to RabbitMQ server: " + Throwables.getStackTraceAsString(e),
                        System.currentTimeMillis());
                close();
                throw new RestartBlockException(true);
            } finally {
                try { csv.close(); } catch (IOException e) {}
            }

            return true;
        }

        @Override
        public void onBlockCompletion() throws RestartBlockException {
            try {
                getChannel().waitForConfirmsOrDie();
            } catch (Exception e) {
                slogger.error("Failed to wait for confirmation in RabbitMQ export client.", e);
                close();
                throw new RestartBlockException(false);
            }
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            m_es.shutdown();
            try {
                m_es.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                Throwables.propagate(e);
            }
            close();
        }

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }
    }
}