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
  def convertResourcePathToFilePath(loader: ClassLoader, localPath: String): String = {
    val localUri = loader.getResource(localPath)
    if (localUri == null) {
      throw new URISyntaxException(localPath, "Path does not exist")
    }
    new File(localUri.toURI).getPath
  }

  @throws[URISyntaxException]
  def getResourceSource(loader: ClassLoader, localPath: String): Source = {
    val filePath = convertResourcePathToFilePath(loader, localPath)
    Source.fromFile(filePath)
  }
}
