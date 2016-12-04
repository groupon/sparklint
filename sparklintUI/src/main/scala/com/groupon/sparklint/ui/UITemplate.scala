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

package com.groupon.sparklint.ui

import scala.xml.Node

/**
  * Similar to any frontend developing technique, this file serve as an HTML template, in scala fashion.
  *
  * @author rxue
  * @since 4/15/16.
  */
trait UITemplate {
  /**
    * Override page title
    */
  val title: String = ""

  val description: String = ""

  val author: String = ""

  final def HTML = <html lang="en">
    <head>
      <meta charset="UTF-8"/>
      <meta http-equiv="X-UA-Compatible" content="IE=edge"/>
      <meta name="viewport" content="width=device-width, initial-scale=1"/>
      <meta name="description" content={description}/>
      <meta name="author" content={author}/>
      <title>
        {title}
      </title>
      <link rel="stylesheet" type="text/css" href="/static/css/bootstrap.min.css"/>
      <link rel="stylesheet" type="text/css" href="/static/css/sb-admin-2.min.css"/>
      <link rel="stylesheet" type="text/css" href="/static/font-awesome/css/font-awesome.min.css"/>
      <link rel="stylesheet" type="text/css" href="/static/css/morris.css"/>
      <link rel="stylesheet" type="text/css" href="/static/css/metismenu.min.css"/>{extraCSS}
    </head>
    <body>
      {content}<script src="/static/js/jquery.min.js"></script>
      <script src="/static/js/bootstrap.min.js"></script>
      <script src="/static/js/raphael.min.js"></script>
      <script src="/static/js/morris.min.js"></script>
      <script src="/static/js/sb-admin-2.min.js"></script>
      <script src="/static/js/moment.min.js"></script>
      <script src="/static/js/metismenu.min.js"></script>
      <script src="/static/js/underscore.min.js"></script>{extraScripts}
    </body>
  </html>

  /**
    * Add extra css here
    *
    * @return
    */
  protected def extraCSS: Seq[Node] = <link rel="stylesheet" type="text/css" href="/static/css/sparklint.css"/>

  /**
    * Add extra js here
    *
    * @return
    */
  protected def extraScripts: Seq[Node] = Seq.empty

  protected def content: Seq[Node] = Seq.empty
}
