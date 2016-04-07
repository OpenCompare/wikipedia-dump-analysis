package org.opencompare.analysis.analyzer

import de.fau.cs.osr.ptk.common.AstVisitor
import org.opencompare.api.java.io.ImportMatrix
import org.sweble.wikitext.parser.{WikitextParser, WikitextPreprocessor}
import org.sweble.wikitext.parser.nodes.{WtExternalLink, WtHorizontalRule, WtTemplate, _}
import org.sweble.wikitext.parser.utils.SimpleParserConfig

/**
  * Created by gbecan on 05/04/16.
  */
class TemplateAnalyzer extends AstVisitor[WtNode] with CompleteWikitextVisitorNoReturn {

  private val parserConfig = new SimpleParserConfig()
  private val preprocessor = new WikitextPreprocessor(parserConfig)
//  private val parser = new WikitextParser(parserConfig)

  private var results = Map.empty[String, Int]
  private var inTemplate = false
  private var templateName = ""

  def analyzeTemplates(importMatrix: ImportMatrix, title : String) : Map[String, Int] = {
    results = Map.empty[String, Int]

    for (row <- 0 until importMatrix.getNumberOfRows;
         column <- 0 until importMatrix.getNumberOfColumns) {
      val cell = Option(importMatrix.getCell(row, column))

      if (cell.isDefined) {
        val code = "{|\n" +
          "|-\n" +
          "| " +
          cell.get.getRawContent + "\n" +
          "|}"

        val ast = preprocessor.parseArticle(code, title)
        this.go(ast)
      }
    }

    results
  }


  override def visit(n: WtTable): Unit = iterate(n)

  override def visit(n: WtTemplate): Unit = {
    inTemplate = true
    templateName = ""
    dispatch(n.getName)
    results = results  + (templateName -> (results.getOrElse(templateName, 0) + 1))
    inTemplate =false
  }

  override def visit(n: WtTemplateArguments): Unit = iterate(n)

  override def visit(n: WtText): Unit = {
    if (inTemplate) {
      templateName = n.getContent
    }
  }


  override def visit(n: WtTableImplicitTableBody): Unit = iterate(n)

  override def visit(n: WtRedirect): Unit = iterate(n)

  override def visit(n: WtLinkOptionLinkTarget): Unit = iterate(n)

  override def visit(n: WtTableHeader): Unit = iterate(n)

  override def visit(n: WtTableRow): Unit = iterate(n)

  override def visit(n: WtTagExtension): Unit = iterate(n)

  override def visit(n: WtUnorderedList): Unit = iterate(n)

  override def visit(n: WtValue): Unit = iterate(n)

  override def visit(n: WtWhitespace): Unit = iterate(n)

  override def visit(n: WtXmlAttributes): Unit = iterate(n)

  override def visit(n: WtIgnored): Unit = iterate(n)

  override def visit(n: WtLinkOptionGarbage): Unit = iterate(n)

  override def visit(n: WtNewline): Unit = iterate(n)

  override def visit(n: WtPageName): Unit = iterate(n)

  override def visit(n: WtTemplateArgument): Unit = iterate(n)

  override def visit(n: WtXmlElement): Unit = iterate(n)

  override def visit(n: WtImageLink): Unit = iterate(n)

  override def visit(n: WtTemplateParameter): Unit = iterate(n)

  override def visit(n: WtHorizontalRule): Unit = iterate(n)

  override def visit(n: WtIllegalCodePoint): Unit = iterate(n)

  override def visit(n: WtLinkOptionKeyword): Unit = iterate(n)

  override def visit(n: WtLinkOptionResize): Unit = iterate(n)

  override def visit(n: WtXmlAttribute): Unit = iterate(n)

  override def visit(n: WtXmlEmptyTag): Unit = iterate(n)

  override def visit(n: WtXmlStartTag): Unit = iterate(n)

  override def visit(n: WtImStartTag): Unit = iterate(n)

  override def visit(n: WtImEndTag): Unit = iterate(n)

  override def visit(n: WtXmlEndTag): Unit = iterate(n)

  override def visit(n: WtXmlCharRef): Unit = iterate(n)

  override def visit(n: WtUrl): Unit = iterate(n)

  override def visit(n: WtTicks): Unit = iterate(n)

  override def visit(n: WtSignature): Unit = iterate(n)

  override def visit(n: WtPageSwitch): Unit = iterate(n)

  override def visit(n: WtTableCell): Unit = iterate(n)

  override def visit(n: WtTableCaption): Unit = iterate(n)

  override def visit(n: WtSection): Unit = iterate(n)

  override def visit(n: WtInternalLink): Unit = iterate(n)

  override def visit(n: WtExternalLink): Unit = iterate(n)

  override def visit(n: WtXmlEntityRef): Unit = iterate(n)

  override def visit(n: WtNodeList): Unit = iterate(n)

  override def visit(n: WtBody): Unit = iterate(n)

  override def visit(n: WtBold): Unit = iterate(n)

  override def visit(n: WtParsedWikitextPage): Unit = iterate(n)

  override def visit(n: WtOrderedList): Unit = iterate(n)

  override def visit(n: WtOnlyInclude): Unit = iterate(n)

  override def visit(n: WtName): Unit = iterate(n)

  override def visit(n: WtListItem): Unit = iterate(n)

  override def visit(n: WtLinkTitle): Unit = iterate(n)

  override def visit(n: WtLinkOptions): Unit = iterate(n)

  override def visit(n: WtLinkOptionAltText): Unit = iterate(n)

  override def visit(n: WtItalics): Unit = iterate(n)

  override def visit(n: WtHeading): Unit = iterate(n)

  override def visit(n: WtDefinitionListTerm): Unit = iterate(n)

  override def visit(n: WtDefinitionListDef): Unit = iterate(n)

  override def visit(n: WtDefinitionList): Unit = iterate(n)

  override def visit(n: WtPreproWikitextPage): Unit = iterate(n)

  override def visit(n: WtParagraph): Unit = iterate(n)

  override def visit(n: WtSemiPre): Unit = iterate(n)

  override def visit(n: WtSemiPreLine): Unit = iterate(n)

  override def visit(n: WtXmlComment): Unit = iterate(n)

  override def visit(n: WtXmlAttributeGarbage): Unit = iterate(n)

  override def visit(n: WtTagExtensionBody): Unit = iterate(n)
}
