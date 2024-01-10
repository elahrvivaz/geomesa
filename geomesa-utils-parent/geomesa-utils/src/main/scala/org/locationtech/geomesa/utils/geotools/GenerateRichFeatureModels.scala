/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{File, FileWriter, Writer}

object GenerateRichFeatureModels {

  /**
   * Creates implicit wrappers for any typesafe config format files found under src/main/resources
   *
   * @param args (0) - base directory for the maven project
   *             (1) - package to place the implicit classes
   */
  def main(args: Array[String]): Unit = {
    val basedir = args(0)
    val packageName = args(1)
    assert(basedir != null)
    assert(packageName != null)

    val sfts = SimpleFeatureTypeLoader.sfts

    if (sfts.isEmpty) {
      println("No formats found")
    } else {
      sfts.foreach { sft =>
        val name = s"${safeIdentifier(sft.getTypeName)}Model"
        val classFilePath = s"$basedir/src/main/scala/${packageName.replaceAll("\\.", "/")}/$name.scala"
        val classFile = new File(classFilePath)
        println(s"Writing class file $packageName.$name with feature type ${sft.getTypeName}")
        WithClose(new FileWriter(classFile)) { fw =>
          buildSingleClass(sft, name, packageName, fw)
        }
      }
    }
  }

  def buildSingleClass(sft: SimpleFeatureType, name: String, pkg: String, out: Writer): Unit = {
    out.append(s"package $pkg\n\n")
    out.append("import org.geotools.api.feature.simple.SimpleFeature\n\n")
    out.append(s"// this class was generated by ${GenerateRichFeatureModels.getClass.getName} for feature type ${sft.getTypeName}\n")
    out.append(s"object $name {\n\n")
    out.append(s"  implicit class RichFeature(val sf: SimpleFeature) extends AnyVal {\n")
    var i = 0
    while (i < sft.getAttributeCount) {
      val attr = AttributeMethods(sft, i)
      out.append("\n")
      out.append(s"    ${attr.getter}\n")
      out.append(s"    ${attr.optionGetter}\n")
      out.append(s"    ${attr.setter}\n")
      i += 1
    }
    out.append(
      s"""
         |    def debug(): String = {
         |      import scala.collection.JavaConverters._
         |      val sb = new StringBuilder(s"$${sf.getType.getTypeName}:$${sf.getID}")
         |      sf.getProperties.asScala.foreach(p => sb.append(s"|$${p.getName.getLocalPart}=$${p.getValue}"))
         |      sb.toString()
         |    }
         |""".stripMargin)
    out.append("  }\n}")
  }

  private[geotools] def getAttributeBinding(descriptor: AttributeDescriptor): String = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._

    def subtype(c: Class[_]): String = c match {
      case null => "String"
      case c => c.getCanonicalName
    }

    val binding = descriptor.getType.getBinding.getCanonicalName
    if (descriptor.isList) {
      s"$binding[${subtype(descriptor.getListType())}]"
    } else if (descriptor.isMap) {
      val (keyType, valueType) = descriptor.getMapTypes()
      s"$binding[${subtype(keyType)},${subtype(valueType)}]"
    } else {
      binding
    }
  }

  private def safeIdentifier(name: String): String = {
    val safe = new StringBuilder()
    var i = 0
    var upper = true // camel case
    while (i < name.length) {
      val char = name.charAt(i)
      if (char == '_'||
          (i == 0 && !Character.isJavaIdentifierStart(char)) ||
          (i != 0 && !Character.isJavaIdentifierPart(char))) {
        upper = true
      } else if (upper) {
        safe.append(char.toUpper)
        upper = false
      } else {
        safe.append(char)
      }
      i += 1
    }
    if (safe.isEmpty) {
      throw new IllegalArgumentException(s"Can't create valid Java identifier from $name")
    }
    safe.toString
  }

  private class AttributeMethods(name: String, index: Int, clazz: String) {
    def getter: String = s"def get$name: $clazz = sf.getAttribute($index).asInstanceOf[$clazz]"
    def optionGetter: String = s"def opt$name: Option[$clazz] = Option(get$name)"
    def setter: String = s"def set$name(x: $clazz): Unit = sf.setAttribute($index, x)"
  }

  private object AttributeMethods {
    def apply(sft: SimpleFeatureType, i: Int): AttributeMethods = {
      val descriptor = sft.getDescriptor(i)
      val binding = getAttributeBinding(descriptor)
      new AttributeMethods(safeIdentifier(descriptor.getLocalName), i, binding)
    }
  }
}
