package com.groupon.sparklint

import org.scalatest.Tag

import com.groupon.sparklint.common.{Scheduler, SparklintConfig}
import com.groupon.sparklint.events.EventSourceManager
import org.scalatest.FlatSpec

/**
  * @author swhitear
  * @since 9/12/16.
  */
object UIPreviewTest extends Tag("UIPreview")

class UIWithFullDirectoryReplay extends FlatSpec {

  it should "preview UI with directory source" taggedAs UIPreviewTest in {
    val evSourceManager = new EventSourceManager()
    val scheduler = new Scheduler()
    val args = Array("--directory", "src/test/resources/directory_source")
    val config =SparklintConfig(exitOnError = false).parseCliArgs(args)
    val ui = new SparklintServer(evSourceManager, scheduler, config)
    ui.run()
    SparklintServer.waitForever
  }

}

