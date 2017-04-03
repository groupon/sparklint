package com.groupon.sparklint.event

import java.io.{File, FileNotFoundException}

/**
  * @throws FileNotFoundException if the folder provided doesn't exist or is a file
  * @param folder the folder of the log files
  * @author rxue
  * @since 1.0.5
  */
@throws[FileNotFoundException]
class FolderEventSourceGroupManager(folder: File) extends GenericEventSourceGroupManager(folder.getName, true) {
  if (!folder.exists() || folder.isFile) {
    throw new FileNotFoundException(folder.getAbsolutePath)
  }

}
