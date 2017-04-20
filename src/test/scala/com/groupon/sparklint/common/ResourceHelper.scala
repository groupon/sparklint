/*
 * Copyright 2016 Groupon, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.groupon.sparklint.common

import java.io.File
import java.net.URISyntaxException

import scala.io.Source

/**
  * @author rxue
  * @since 6/16/16.
  */
object ResourceHelper {
  @throws[URISyntaxException]
  def getResourceSource(loader: ClassLoader, localPath: String): Source = {
    val filePath = convertResourcePathToFilePath(loader, localPath)
    Source.fromFile(filePath)
  }

  @throws[URISyntaxException]
  def convertResourcePathToFilePath(loader: ClassLoader, localPath: String): String = {
    val localUri = loader.getResource(localPath)
    if (localUri == null) {
      throw new URISyntaxException(localPath, "Path does not exist")
    }
    new File(localUri.toURI).getPath
  }
}
