/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.server;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.network.ExtendedChannelPromise;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.spark.network.TestManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import sun.rmi.runtime.Log;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

public class OneForOneStreamManagerSuite {

  List<ManagedBuffer> managedBuffersToRelease = new ArrayList<>();

  @After
  public void tearDown() {
    managedBuffersToRelease.forEach(managedBuffer -> managedBuffer.release());
    managedBuffersToRelease.clear();
  }

  private ManagedBuffer getChunk(OneForOneStreamManager manager, long streamId, int chunkIndex) {
    ManagedBuffer chunk = manager.getChunk(streamId, chunkIndex);
    if (chunk != null) {
      managedBuffersToRelease.add(chunk);
    }
    return chunk;
  }

  @Test
  public void testMissingChunk() {
    OneForOneStreamManager manager = new OneForOneStreamManager();
    List<ManagedBuffer> buffers = new ArrayList<>();
    TestManagedBuffer buffer1 = Mockito.spy(new TestManagedBuffer(10));
    TestManagedBuffer buffer2 = Mockito.spy(new TestManagedBuffer(20));
    TestManagedBuffer buffer3 = Mockito.spy(new TestManagedBuffer(20));

    buffers.add(buffer1);
    // the nulls here are to simulate a file which goes missing before being read,
    // just as a defensive measure
    buffers.add(null);
    buffers.add(buffer2);
    buffers.add(null);
    buffers.add(buffer3);

    Channel dummyChannel = Mockito.mock(Channel.class, Mockito.RETURNS_SMART_NULLS);
    long streamId = manager.registerStream("appId", buffers.iterator(), dummyChannel);
    Assert.assertEquals(1, manager.numStreamStates());
    Assert.assertNotNull(getChunk(manager, streamId, 0));
    Assert.assertNull(getChunk(manager, streamId, 1));
    Assert.assertNotNull(getChunk(manager, streamId, 2));
    manager.connectionTerminated(dummyChannel);

    // loaded buffers are not released yet as in production a MangedBuffer returned by getChunk()
    // would only be released by Netty after it is written to the network
    Mockito.verify(buffer1, Mockito.never()).release();
    Mockito.verify(buffer2, Mockito.never()).release();
    Mockito.verify(buffer3, Mockito.times(1)).release();
  }

  @Test
  public void managedBuffersAreFreedWhenConnectionIsClosed() {
    OneForOneStreamManager manager = new OneForOneStreamManager();
    List<ManagedBuffer> buffers = new ArrayList<>();
    TestManagedBuffer buffer1 = Mockito.spy(new TestManagedBuffer(10));
    TestManagedBuffer buffer2 = Mockito.spy(new TestManagedBuffer(20));
    buffers.add(buffer1);
    buffers.add(buffer2);

    Channel dummyChannel = Mockito.mock(Channel.class, Mockito.RETURNS_SMART_NULLS);
    manager.registerStream("appId", buffers.iterator(), dummyChannel);
    Assert.assertEquals(1, manager.numStreamStates());
    manager.connectionTerminated(dummyChannel);

    Mockito.verify(buffer1, Mockito.times(1)).release();
    Mockito.verify(buffer2, Mockito.times(1)).release();
    Assert.assertEquals(0, manager.numStreamStates());
  }

  @Test
  public void streamStatesAreFreedWhenConnectionIsClosedEvenIfBufferIteratorThrowsException() {
    OneForOneStreamManager manager = new OneForOneStreamManager();

    Iterator<ManagedBuffer> buffers = Mockito.mock(Iterator.class);
    Mockito.when(buffers.hasNext()).thenReturn(true);
    Mockito.when(buffers.next()).thenThrow(RuntimeException.class);

    ManagedBuffer mockManagedBuffer = Mockito.mock(ManagedBuffer.class);

    Iterator<ManagedBuffer> buffers2 = Mockito.mock(Iterator.class);
    Mockito.when(buffers2.hasNext()).thenReturn(true).thenReturn(true);
    Mockito.when(buffers2.next()).thenReturn(mockManagedBuffer).thenThrow(RuntimeException.class);

    Channel dummyChannel = Mockito.mock(Channel.class, Mockito.RETURNS_SMART_NULLS);
    manager.registerStream("appId", buffers, dummyChannel);
    manager.registerStream("appId", buffers2, dummyChannel);

    Assert.assertEquals(2, manager.numStreamStates());

    try {
      manager.connectionTerminated(dummyChannel);
      Assert.fail("connectionTerminated should throw exception when fails to release all buffers");

    } catch (RuntimeException e) {

      Mockito.verify(buffers, Mockito.times(1)).hasNext();
      Mockito.verify(buffers, Mockito.times(1)).next();

      Mockito.verify(buffers2, Mockito.times(2)).hasNext();
      Mockito.verify(buffers2, Mockito.times(2)).next();

      Mockito.verify(mockManagedBuffer, Mockito.times(1)).release();

      Assert.assertEquals(0, manager.numStreamStates());
    }
  }

  @Test
  public void testChunkBenchmark() {
    RpcHandler rpcHandler = new NoOpRpcHandler();
    OneForOneStreamManager manager = (OneForOneStreamManager) (rpcHandler.getStreamManager());
    Map<Integer, List<Long>> streamsSizeList = new HashMap<>();
    streamsSizeList.put(10000, new ArrayList<>());
    streamsSizeList.put(50000, new ArrayList<>());
    streamsSizeList.put(100000, new ArrayList<>());
    int retryTimes = 10;



    ExecutorService pool = Executors.newFixedThreadPool(100,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TestChunkBenchMark" + "-%d").build());

    for (int retry = 0; retry < retryTimes; retry++) {
      Map<Integer, Runnable> requests = new HashMap<>();
      Map<Integer, List<Pair<Object, ExtendedChannelPromise>>> responses = new HashMap<>();
      List<Future> tasks = new ArrayList<>();
      for (int streamsSize : streamsSizeList.keySet()) {
        for (int index = 0; index < streamsSize; index++) {
          ChannelHandlerContext context = mock(ChannelHandlerContext.class);
          Channel channel = mock(Channel.class);
          when(context.channel())
              .thenAnswer(invocationOnMock0 -> channel);
          List<Pair<Object, ExtendedChannelPromise>> responseAndPromisePairs =
              new ArrayList<>();
          when(channel.writeAndFlush(any()))
              .thenAnswer(invocationOnMock0 -> {
                Object response = invocationOnMock0.getArguments()[0];
                ExtendedChannelPromise channelFuture = new ExtendedChannelPromise(channel);
                responseAndPromisePairs.add(ImmutablePair.of(response, channelFuture));
                return channelFuture;
              });
          long streamId = registerStream(channel, manager);
          Runnable run = () -> {
            try {
              TransportClient reverseClient = mock(TransportClient.class);
              ChunkFetchRequestHandler requestHandler = new ChunkFetchRequestHandler(reverseClient,
                  rpcHandler.getStreamManager(), Long.MAX_VALUE, false);
              RequestMessage request0 = new ChunkFetchRequest(new StreamChunkId(streamId, 0));
              requestHandler.channelRead(context, request0);
              RequestMessage request1 = new ChunkFetchRequest(new StreamChunkId(streamId, 1));
              requestHandler.channelRead(context, request1);
              RequestMessage request2 = new ChunkFetchRequest(new StreamChunkId(streamId, 2));
              requestHandler.channelRead(context, request2);
            } catch (Exception e) {
              throw new RuntimeException("Request failed", e.getCause());
            }
          };
          requests.put(index, run);
          responses.put(index, responseAndPromisePairs);
        }

        Assert.assertEquals(streamsSize, manager.numStreamStates());
        Long start = System.currentTimeMillis();
        for (Runnable request : requests.values()) {
          tasks.add(pool.submit(request));
        }
        for (Future task : tasks) {
          try {
            task.get();
          } catch (InterruptedException e) {
            e.printStackTrace();
          } catch (ExecutionException e) {
            e.printStackTrace();
          }
        }
        streamsSizeList.get(streamsSize).add(System.currentTimeMillis() - start);
        for (List<Pair<Object, ExtendedChannelPromise>> result : responses.values()) {
          Assert.assertEquals(3 , result.size());
        }
      }
    }

    System.out.println("OneForOneStreamManager fetch data duration test:");
    System.out.println("Stream Size          Max           Min           Avg");
    for (int streamsSize : streamsSizeList.keySet()) {
      System.out.println(streamsSize + "           " +
          streamsSizeList.get(streamsSize).stream().mapToLong(x -> x).max().getAsLong() + "           " +
          streamsSizeList.get(streamsSize).stream().mapToLong(x -> x).min().getAsLong() + "           " +
          streamsSizeList.get(streamsSize).stream().mapToLong(x -> x).average().getAsDouble());
    }
  }

  private long registerStream(Channel channel, OneForOneStreamManager manager) {
    List<ManagedBuffer> managedBuffers = new ArrayList<>();
    managedBuffers.add(new TestManagedBuffer(10));
    managedBuffers.add(new TestManagedBuffer(20));
    managedBuffers.add(new TestManagedBuffer(30));
    long streamId = manager.registerStream("test-app", managedBuffers.iterator(), channel);
    return streamId;
  }

  /**
   * With patch:
   * OneForOneStreamManager fetch data duration test:
   * Stream Size          Max           Min           Avg
   * 10000           1172           189           444.8
   * 50000           4594           1204           2260.4
   * 100000           7081           2531           4813.0
   *
   * Process finished with exit code 0
   *
   * Without patch:
   * OneForOneStreamManager fetch data duration test:
   * Stream Size      Max           Min           Avg
   * 10000           2100           733           1203.0
   * 50000           26148          20125         22594.5
   * 100000          153909         95787         130210.3
   *
   * Process finished with exit code 0
   */
}

