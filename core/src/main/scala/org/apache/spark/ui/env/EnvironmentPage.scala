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

package org.apache.spark.ui.env

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.SparkConf
import org.apache.spark.status.AppStatusStore
import org.apache.spark.ui._
import org.apache.spark.util.Utils

private[ui] class EnvironmentPage(
    parent: EnvironmentTab,
    conf: SparkConf,
    store: AppStatusStore) extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val appEnv = store.environmentInfo()
    val jvmInformation = Map(
      "Java Version" -> appEnv.runtime.javaVersion,
      "Java Home" -> appEnv.runtime.javaHome,
      "Scala Version" -> appEnv.runtime.scalaVersion)
    // 利用 UIUtils.listingTable 方法，生成JVM运行信息、spark属性信息、系统属性信息、类路径信息的表格
    val runtimeInformationTable = UIUtils.listingTable(
      propertyHeader, jvmRow, jvmInformation, fixedWidth = true)
    val sparkPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      Utils.redact(conf, appEnv.sparkProperties.toSeq), fixedWidth = true)
    val systemPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,
      Utils.redact(conf, appEnv.systemProperties.sorted), fixedWidth = true)
    val classpathEntriesTable = UIUtils.listingTable(
      classPathHeaders, classPathRow, appEnv.classpathEntries, fixedWidth = true)
    val content =
      <span>
        <span class="collapse-aggregated-runtimeInformation collapse-table"
            onClick="collapseTable('collapse-aggregated-runtimeInformation',
            'aggregated-runtimeInformation')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Runtime Information</a>
          </h4>
        </span>
        <div class="aggregated-runtimeInformation collapsible-table">
          {runtimeInformationTable}
        </div>
        <span class="collapse-aggregated-sparkProperties collapse-table"
            onClick="collapseTable('collapse-aggregated-sparkProperties',
            'aggregated-sparkProperties')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Spark Properties</a>
          </h4>
        </span>
        <div class="aggregated-sparkProperties collapsible-table">
          {sparkPropertiesTable}
        </div>
        <span class="collapse-aggregated-systemProperties collapse-table"
            onClick="collapseTable('collapse-aggregated-systemProperties',
            'aggregated-systemProperties')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>System Properties</a>
          </h4>
        </span>
        <div class="aggregated-systemProperties collapsible-table">
          {systemPropertiesTable}
        </div>
        <span class="collapse-aggregated-classpathEntries collapse-table"
            onClick="collapseTable('collapse-aggregated-classpathEntries',
            'aggregated-classpathEntries')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a>Classpath Entries</a>
          </h4>
        </span>
        <div class="aggregated-classpathEntries collapsible-table">
          {classpathEntriesTable}
        </div>
      </span>
    // 调用 UIUtils.headerSparkPage 方法封装好css、js、header及页面布局等
    UIUtils.headerSparkPage(request, "Environment", content, parent)
  }
  // 定义JVM运行时信息、Spark属性信息、系统属性信息的表格头部propertyHeader 和 路径信息的表格头部 classPathHeaders
  private def propertyHeader = Seq("Name", "Value")
  private def classPathHeaders = Seq("Resource", "Source")
  // 定义JVM运行时信息的表格中每行数据的生成方法jvmRow
  private def jvmRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>
}

private[ui] class EnvironmentTab(
    parent: SparkUI,
    store: AppStatusStore) extends SparkUITab(parent, "environment") {
  attachPage(new EnvironmentPage(this, parent.conf, store))
}
