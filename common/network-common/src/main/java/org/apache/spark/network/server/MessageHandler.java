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

import org.apache.spark.network.protocol.Message;

/**
 * Handles either request or response messages coming off of Netty. A MessageHandler instance
 * is associated with a single Netty Channel (though it may have multiple clients on the same
 * Channel.)
 *
 * TransportRequestHandler 和 TransportResponseHandler都继承该类，定义了子类的规范
 *
 * 同时 MessageHandler 也是泛型类
 */
public abstract class MessageHandler<T extends Message> {
  /** 用于对接收到的单个消息进行处理 . */
  public abstract void handle(T message) throws Exception;

  /** 当channel激活时调用. */
  public abstract void channelActive();

  /** 当捕获到channel发生异常时调用. */
  public abstract void exceptionCaught(Throwable cause);

  /** 当channel非激活时调用. */
  public abstract void channelInactive();
}
