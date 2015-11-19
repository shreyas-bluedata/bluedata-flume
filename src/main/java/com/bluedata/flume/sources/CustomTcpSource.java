/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bluedata.flume.sources;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Overly simplified from flume's NetcatSource. Uses streams instead of channels
 * Constructs 100 byte events.
 *
 */
public class CustomTcpSource extends AbstractSource implements Configurable,
    EventDrivenSource {

  private static final Logger logger = LoggerFactory
      .getLogger(CustomTcpSource.class);

  private String hostName;
  private int port;

  private CounterGroup counterGroup;
  private ServerSocket serverSocket;
  private AtomicBoolean acceptThreadShouldStop;
  private Thread acceptThread;
  private ExecutorService handlerService;

  public CustomTcpSource() {
    super();

    port = 0;
    counterGroup = new CounterGroup();
    acceptThreadShouldStop = new AtomicBoolean(false);
  }

  @Override
  public void configure(Context context) {
    String hostKey = "bind";
    String portKey = "port";

    Configurables.ensureRequiredNonNull(context, hostKey, portKey);

    hostName = context.getString(hostKey);
    port = context.getInteger(portKey);
    }

  @Override
  public void start() {

    logger.info("Source starting");

    counterGroup.incrementAndGet("open.attempts");

    handlerService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat("netcat-handler-%d").build());

    try {
      SocketAddress bindPoint = new InetSocketAddress(hostName, port);

      serverSocket = new ServerSocket();
      serverSocket.setReuseAddress(true);
      serverSocket.bind(bindPoint);
      logger.info("Created serverSocket:{}", serverSocket);
    } catch (IOException e) {
      counterGroup.incrementAndGet("open.errors");
      logger.error("Unable to bind to socket. Exception follows.", e);
      throw new FlumeException(e);
    }

    AcceptHandler acceptRunnable = new AcceptHandler();
    acceptThreadShouldStop.set(false);
    acceptRunnable.counterGroup = counterGroup;
    acceptRunnable.handlerService = handlerService;
    acceptRunnable.shouldStop = acceptThreadShouldStop;
    acceptRunnable.source = this;
    acceptRunnable.serverSocket = serverSocket;

    acceptThread = new Thread(acceptRunnable);

    acceptThread.start();

    logger.debug("Source started");
    super.start();
  }

  @Override
  public void stop() {
    logger.info("Source stopping");

    acceptThreadShouldStop.set(true);

    if (acceptThread != null) {
      logger.debug("Stopping accept handler thread");

      while (acceptThread.isAlive()) {
        try {
          logger.debug("Waiting for accept handler to finish");
          acceptThread.interrupt();
          acceptThread.join(500);
        } catch (InterruptedException e) {
          logger
              .debug("Interrupted while waiting for accept handler to finish");
          Thread.currentThread().interrupt();
        }
      }

      logger.debug("Stopped accept handler thread");
    }

    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        logger.error("Unable to close socket. Exception follows.", e);
        return;
      }
    }

    if (handlerService != null) {
      handlerService.shutdown();

      logger.debug("Waiting for handler service to stop");

      // wait 500ms for threads to stop
      try {
        handlerService.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger
            .debug("Interrupted while waiting for netcat handler service to stop");
        Thread.currentThread().interrupt();
      }

      if (!handlerService.isShutdown()) {
        handlerService.shutdownNow();
      }

      logger.debug("Handler service stopped");
    }

    logger.debug("Source stopped. Event metrics:{}", counterGroup);
    super.stop();
  }

  private static class AcceptHandler implements Runnable {

    private ServerSocket serverSocket;
    private CounterGroup counterGroup;
    private ExecutorService handlerService;
    private EventDrivenSource source;
    private AtomicBoolean shouldStop;

    public AcceptHandler() {
    }

    @Override
    public void run() {
      logger.debug("Starting accept handler");

      while (!shouldStop.get()) {
        try {
          Socket socket = serverSocket.accept();

          NetcatSocketHandler request = new NetcatSocketHandler(this);

          request.socket = socket;
          request.counterGroup = counterGroup;
          request.source = source;

          handlerService.submit(request);

          counterGroup.incrementAndGet("accept.succeeded");
        } catch (ClosedByInterruptException e) {
          // Parent is canceling us.
        } catch (IOException e) {
          logger.error("Unable to accept connection. Exception follows.", e);
          counterGroup.incrementAndGet("accept.failed");
        }
      }

      logger.debug("Accept handler exiting");
    }
  }

  private static class NetcatSocketHandler implements Runnable {

    private Source source;
    private CounterGroup counterGroup;
    private Socket socket;

    private AcceptHandler acceptHandler;

    public NetcatSocketHandler(AcceptHandler acceptHandler) {
      this.acceptHandler = acceptHandler;
    }

    @Override
    public void run() {
      logger.debug("Starting connection handler");

      try {
    	DataInputStream stream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
    	ByteBuffer buffer = ByteBuffer.allocate(100);
    	buffer.clear();
    	boolean keep_running = true;
    	while(keep_running){
    		try{
    			stream.readFully(buffer.array());
    		} catch(EOFException eof){
    			logger.info("EOF exception from stream. Completing this session");
    			stream.read(buffer.array());
    			keep_running = false;
    		}
    		int numProcessed = processEvents(buffer.asCharBuffer());
    		logger.trace("Number of events:" + numProcessed);
    		buffer.clear();
    	}
        counterGroup.incrementAndGet("sessions.completed");
      } catch (Exception e) {
        counterGroup.incrementAndGet("sessions.broken");
        logger.error("Shreyas: Some exception in socket receive.Terminating TCP source.");
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        sw.flush();
        logger.error(sw.toString());
        acceptHandler.shouldStop.set(true);
      } finally {
    	try {
			socket.close();
		} catch (IOException e) {
			logger.error("IOException closing the socket");
		}
      }

      logger.debug("Connection handler exiting");
    }

    /**
     * <p>Consume some number of events from the buffer into the system.</p>
     *
     * Invariants (pre- and post-conditions): <br/>
     *   buffer should have position @ beginning of unprocessed data. <br/>
     *   buffer should have limit @ end of unprocessed data. <br/>
     *
     * @param buffer The buffer containing data to process
     * @param writer The channel back to the client
     * @return number of events successfully processed
     * @throws IOException
     */
    private int processEvents(CharBuffer buffer)
        throws IOException {

      int numProcessed = 0;
      ByteBuffer bytes = Charsets.UTF_8.encode(buffer);
      byte[] body = new byte[bytes.remaining()];
      bytes.get(body);
      Event event = EventBuilder.withBody(body);

      // process event
      ChannelException ex = null;
      try {
        source.getChannelProcessor().processEvent(event);
      } catch (ChannelException chEx) {
        ex = chEx;
      }

      if (ex == null) {
        counterGroup.incrementAndGet("events.processed");
        numProcessed++;
      } else {
        counterGroup.incrementAndGet("events.failed");
        logger.warn("Error processing event. Exception follows.", ex);
      }

      return numProcessed;
    }
  }
}