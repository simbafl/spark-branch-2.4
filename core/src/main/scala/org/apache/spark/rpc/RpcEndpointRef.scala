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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

/**
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 */
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {
  // RPC最大重新连接次数，可以使用 spark.rpc.numRetries 属性进行配置，默认3次
  private[this] val maxRetries = RpcUtils.numRetries(conf);
  // RPC每次重新连接需要等待的毫秒数，可以使用 spark.rpc.netty.wait 属性进行配置，默认3秒
  private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf);
  // RPC的ask操作的默认超时时间，可以使用 spark.rpc.askTimeout 或 spark.network.timeout 属性进行配置，默认配置120秒
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf);

  /**
   * return the address for the [[RpcEndpointRef]]
   * 返回 RpcEndpointRef 对应 RpcEndpoint的RPC地址
   */
  def address: RpcAddress;

  def name: String;

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   * 发送单向异步的消息
   * 采用 at-most-once 的投递规则
   */
  def send(message: Any): Unit;

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
   *
   * 发送消息并同时接收Future
   */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T];

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * default timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
   * loop of [[RpcEndpoint]].

   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout);

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * specified timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   *
   * 发送同步消息，此类请求将会被RpcEndpoint接收，并在规定时间内等待返回类型为 T 的结果
   */
  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }

}
