/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.features.kryo.json

import org.locationtech.geomesa.features.kryo.json.JsonPathParser._
import org.parboiled.Context
import org.parboiled.errors.{ErrorUtils, ParsingException}
import org.parboiled.scala._

object JsonPathParser {

  private val Parser = new JsonPathParser()

  @throws(classOf[ParsingException])
  def parse(path: String, report: Boolean = true): JsonPath = {
    val runner = if (report) { ReportingParseRunner(Parser.Path) } else { BasicParseRunner(Parser.Path) }
    val parsing = runner.run(path)
    parsing.result.getOrElse {
      throw new ParsingException(s"Invalid JSON source:\n${ErrorUtils.printParseErrors(parsing)}")
    }
  }

  case class JsonPath(elements: Seq[PathElement])

  sealed trait PathElement

  case class PathAttribute(name: String) extends PathElement

  case class PathIndex(index: Int) extends PathElement

  case class PathIndices(indices: Seq[Int]) extends PathElement

  case object PathAttributeWildCard extends PathElement

  case object PathIndexWildCard extends PathElement

  case object PathDeepScan extends PathElement
}

private class JsonPathParser extends Parser {
  // main parsing rule
  def Path: Rule1[JsonPath] = rule { "$" ~ zeroOrMore(Element) ~ EOI ~~> JsonPath }

  def Element: Rule1[PathElement] = rule {
    Attribute | ArrayIndex | ArrayIndices | AttributeWildCard | IndexWildCard | DeepScan
  }

  def IndexWildCard: Rule1[PathElement] = rule { "[*]" ~ push(PathIndexWildCard) }

  def AttributeWildCard: Rule1[PathElement] = rule { ".*" ~ push(PathAttributeWildCard) }

  // we have to push the deep scan directly onto the stack as there is no forward matching and
  // it's ridiculous trying to combine Rule1's and Rule2's
  def DeepScan: Rule1[PathElement] = rule { "." ~ toRunAction(pushDeepScan) ~ (Attribute | AttributeWildCard) }

  // note: this assumes that we are inside a zeroOrMore, which is always the case so far
  private def pushDeepScan(context: Context[Any]): Unit =
    context.getValueStack.push(PathDeepScan :: context.getValueStack.pop.asInstanceOf[List[_]])

  def ArrayIndex: Rule1[PathIndex] = rule { "[" ~ Number ~ "]" ~~> PathIndex }

  def ArrayIndices: Rule1[PathIndices] =
    rule { "[" ~ Number ~ "," ~ oneOrMore(Number, ",") ~ "]" ~~> ((n0, n) => PathIndices(n.+:(n0))) }

  def Attribute: Rule1[PathAttribute] = rule { "." ~ oneOrMore(Character) ~> PathAttribute }

  def Number: Rule1[Int] = rule { oneOrMore("0" - "9") ~> (_.toInt) }

  def Character: Rule0 = rule { EscapedChar | NormalChar }

  def EscapedChar: Rule0 = rule { "\\" ~ (anyOf("\"\\/bfnrt") | Unicode) }

  def NormalChar: Rule0 = rule { "a" - "z" | "A" - "Z" | "0" - "9" }

  def Unicode: Rule0 = rule { "u" ~ HexDigit ~ HexDigit ~ HexDigit ~ HexDigit }

  def HexDigit: Rule0 = rule { "0" - "9" | "a" - "f" | "A" - "Z" }
}