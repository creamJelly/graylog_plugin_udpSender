/**
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.graylog.udpsender.senders;

import com.graylog.udpsender.UDPSenderThread;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.string.StringEncoder;
import org.graylog2.plugin.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UDPSender implements Sender {

    private static final Logger LOG = LoggerFactory.getLogger(UDPSender.class);

    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s");

    private final String hostname;
    private final int port;
    private String params;
    private String separator;

    boolean initialized = false;

    int times = 0;

    protected final BlockingQueue<DatagramPacket> queue;

    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    public UDPSender(String hostname, int port, String params, String separator) {
        this.hostname = hostname;
        this.port = port;
        this.params = params;
        this.separator = separator;

        /*
          * This internal queue shields us from causing OutputBufferProcessor
          * timeouts for a short time without risking memory overload or
          * loosing messages in case of temporary connection problems.
          *
          * TODO: Make configurable.
          */
        this.queue = new LinkedBlockingQueue<>(512);
    }

    protected void createBootstrap(final EventLoopGroup workerGroup) {
        final Bootstrap bootstrap = new Bootstrap();
        final UDPSenderThread senderThread = new UDPSenderThread(queue);

        bootstrap.group(workerGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .remoteAddress(new InetSocketAddress(hostname, port))
                .handler(new ChannelInitializer<NioDatagramChannel>() {
                    @Override
                    protected void initChannel(NioDatagramChannel ch) throws Exception {
                        ch.pipeline().addLast(new StringEncoder());

                        ch.pipeline().addLast(new SimpleChannelInboundHandler<NioDatagramChannel>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, NioDatagramChannel msg) throws Exception {
                              // we only send data, never read on the socket
                            }

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                senderThread.start(ctx.channel());
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                LOG.info("Channel disconnected.");
                                senderThread.stop();
                                scheduleReconnect(ctx.channel().eventLoop());
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                LOG.error("Exception caught", cause);
                            }
                        });
                    }
                });
        bootstrap.bind(0).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    LOG.info("Connected.");
                } else {
                    LOG.error("Connection failed: {}", future.cause().getMessage());
                    scheduleReconnect(future.channel().eventLoop());
                }
            }
        });
//        senderThread.start(channel);
    }

    private void scheduleReconnect(final EventLoopGroup workerGroup) {
        workerGroup.schedule(new Runnable() {
            @Override
            public void run() {
                times += 1;
                if (times <= 5) {
                    LOG.info("Starting reconnect!");
                    createBootstrap(workerGroup);
                }
                else {
                    LOG.info("reconnect over 5 times, stop!");
                }
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void initialize() {
        createBootstrap(workerGroup);

        initialized = true;
    }

    @Override
    public void stop() {
        workerGroup.shutdownGracefully();
    }

    @Override
    public void send(Message message) {
        StringBuilder splunkMessage = new StringBuilder();
//        String flowName = "";
        LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
        String[] paramList = this.params.split(",");
        for(String param : paramList) {
            map.put(param, "");
        }

        // 给各字段赋值
        for(Map.Entry<String, Object> field : message.getFields().entrySet()) {
            if (map.containsKey(field.getKey())) {
                map.put(field.getKey(), field.getValue().toString());
            }
        }

        for(Map.Entry<String, String> entry : map.entrySet()) {
            if (splunkMessage.length() > 0) {
                splunkMessage.append(this.separator);
            }
            splunkMessage.append(entry.getValue());
        }
        splunkMessage.append("\r\n");

        try {
            String resultStr = splunkMessage.toString();
            LOG.info("ready to make struct     " + resultStr);
            ByteBuf byteBuf = Unpooled.buffer(resultStr.getBytes().length);
            byteBuf.writeBytes(resultStr.getBytes());
            DatagramPacket resultData = new DatagramPacket(byteBuf, new InetSocketAddress(this.hostname, this.port));
            queue.put(resultData);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    private Object noNewLines(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            value = ((String) value).replace("\n", " ").replace("\r", " ");
        }

        return value;
    }

    private Object escape(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            Matcher matcher = WHITESPACE_PATTERN.matcher((String) value);
            if (matcher.find()) {
                value = "\"" + value + "\"";
            }
        }

        return noNewLines(value);
    }

}
