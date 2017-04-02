package com.groupon.sparklint.event

import java.io.{File, FileNotFoundException}

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Roboxue on 2017/4/2.
  */
class FolderEventSourceGroupManagerTest extends FlatSpec with Matchers {
  it should "get name correctly" in {
    val folder = new File(getClass.getClassLoader.getResource("directory_source").getFile)
    val esgm = new FolderEventSourceGroupManager(folder)
    esgm.name shouldBe "directory_source"
  }

  it should "throw up if directory is a file" in {
    intercept[FileNotFoundException] {
      val folder = new File(getClass.getClassLoader.getResource("spark_event_log_example").getFile)
      new FolderEventSourceGroupManager(folder)
    }
  }
}
