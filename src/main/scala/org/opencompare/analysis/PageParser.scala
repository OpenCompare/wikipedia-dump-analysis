package org.opencompare.analysis

import scala.xml.XML

/**
  * Created by gbecan on 12/11/15.
  */
class PageParser {

  def parseDump(doc : String): Page = {
    val pageXML = XML.loadString(doc)
    val revXML = (pageXML \ "revision").head

    val title = (pageXML \ "title").head.text
    val id = (pageXML \ "id").head.text

    val revId = (revXML \ "id").head.text
    val revParentIdOption = (revXML \ "parentid").headOption
    val revParentId = if (revParentIdOption.isDefined) {
      revParentIdOption.get.text
    } else {
      ""
    }
    val timestamp = (revXML \ "timestamp").head.text
    val wikitext = (revXML \ "text").head.text

    val revision = Revision(revId, revParentId, timestamp, wikitext)
    val page = Page(id, title, revision)
    page
  }

}
