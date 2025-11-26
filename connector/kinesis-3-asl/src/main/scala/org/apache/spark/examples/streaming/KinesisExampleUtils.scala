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

package org.apache.spark.examples.streaming

import java.net.URI

import scala.jdk.CollectionConverters._

import software.amazon.awssdk.regions.servicemetadata.KinesisServiceMetadata

private[streaming] object KinesisExampleUtils {
  private val paths = {
    val metadata = new KinesisServiceMetadata
    metadata.regions.asScala.toSeq.map(metadata.endpointFor(_).getPath)
  }

  def getRegionNameByEndpoint(endpoint: String): String = {
    val path = URI.create(endpoint).getPath
    paths.find(path.equals(_)).getOrElse(
      throw new IllegalArgumentException(s"Could not resolve region for endpoint: $endpoint"))
  }
}
