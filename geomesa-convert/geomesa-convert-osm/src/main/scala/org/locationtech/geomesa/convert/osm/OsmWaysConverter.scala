/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.convert.osm

import java.io.{IOException, InputStream}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, FileVisitor, Files, Path}
import java.sql.DriverManager

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Coordinate
import de.topobyte.osm4j.core.model.iface._
import de.topobyte.osm4j.core.model.impl.Node
import de.topobyte.osm4j.pbf.seq.PbfIterator
import de.topobyte.osm4j.xml.dynsax.OsmXmlIterator
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert.Transformers.{EvaluationContext, Expr}
import org.locationtech.geomesa.convert._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.IndexedSeq

class OsmWaysConverter(val targetSFT: SimpleFeatureType,
                       val idBuilder: Expr,
                       val inputFields: IndexedSeq[Field],
                       val userDataBuilder: Map[String, Expr],
                       val validating: Boolean,
                       val pbf: Boolean,
                       val needsMetadata: Boolean) extends ToSimpleFeatureConverter[OsmWay] with LazyLogging {

  private def gf = JTSFactoryFinder.getGeometryFactory
  private val toArray = if (needsMetadata) { OsmField.toArrayWithMetadata _ } else { OsmField.toArrayNoMetadata _ }

  val h2Dir = Files.createTempDirectory("geomesa-convert-h2").toFile
  val connection = {
    Class.forName("org.h2.Driver")
    DriverManager.getConnection(s"jdbc:h2:split:${h2Dir.getAbsolutePath}/osm")
  }

  createNodesTable()

  val insertStatement = connection.prepareStatement("INSERT INTO nodes(id, lon, lat) VALUES (?, ?, ?);")

  override def fromInputType(i: OsmWay): Seq[Array[Any]] = {
    // TODO some ways are marked as 'area' and should be polygons?
    // note: nodes may occur more than once in the way
    val nodeIds = Seq.range(0, i.getNumberOfNodes).map(i.getNodeId)
    val selected = selectNodes(nodeIds)
    val nodes = nodeIds.flatMap(selected.get)
    if (nodes.size != nodeIds.length) {
      logger.warn(s"Dropping references to non-existing nodes by way '${i.getId}': " +
          s"${nodeIds.filterNot(nodes.contains).mkString(", ")}")
    }
    if (nodes.size < 2) {
      logger.warn(s"Dropping way '${i.getId}' because it does not have enough valid nodes to form a linestring")
      Seq.empty
    } else {
      val coords = nodes.map(n => new Coordinate(n.getLongitude, n.getLatitude))
      Seq(toArray(i, gf.createLineString(coords.toArray)))
    }
  }

  override def process(is: InputStream, ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature] = {
    val iterator = if (pbf) new PbfIterator(is, needsMetadata) else new OsmXmlIterator(is, needsMetadata)
    def nextElement = if (iterator.hasNext) iterator.next else null
    var element = nextElement
    // first read in all the elements and store them for later lookup
    while (element != null && element.getType == EntityType.Node) {
      insertNode(element.getEntity.asInstanceOf[OsmNode])
      element = nextElement
    }
    val entities = new Iterator[OsmWay] {
      // types are ordered in the file, so we can stop when we hit a non-way type
      override def hasNext: Boolean = element != null && element.getType == EntityType.Way
      override def next(): OsmWay = {
        val ret = element.getEntity.asInstanceOf[OsmWay]
        element = nextElement
        ret
      }
    }
    processInput(entities, ec)
  }

  private def createNodesTable(): Unit = {
    val stmt = connection.prepareStatement("create table nodes(id BIGINT NOT NULL PRIMARY KEY, lon DOUBLE, lat DOUBLE);")
    stmt.execute()
    stmt.close()
  }

  private def insertNode(node: OsmNode): Unit = {
    insertStatement.setLong(1, node.getId)
    insertStatement.setDouble(2, node.getLongitude)
    insertStatement.setDouble(3, node.getLatitude)
    insertStatement.executeUpdate()
  }

  private def selectNodes(ids: Seq[Long]): Map[Long, OsmNode] = {
    val map = scala.collection.mutable.Map.empty[Long, OsmNode]
    // group so that we don't overwhelm the select clause
    ids.grouped(99).foreach { group =>
      val statement = connection.prepareCall(s"SELECT * FROM nodes WHERE id IN(${group.mkString(",")});")
      val results = statement.executeQuery()
      while (results.next()) {
        val node = new Node(results.getLong(1), results.getDouble(2), results.getDouble(3))
        map.put(node.getId, node)
      }
      results.close()
      statement.close()
    }
    map.toMap
  }

  override def close(): Unit = {
    insertStatement.close()
    connection.close()
    Files.walkFileTree(h2Dir.toPath, new FileVisitor[Path] {
      override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (!attrs.isDirectory) {
          Files.delete(file)
        }
        FileVisitResult.CONTINUE
      }
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = FileVisitResult.CONTINUE
      override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
        Files.delete(dir)
        FileVisitResult.CONTINUE
      }
    })
  }
}

class OsmWaysConverterFactory extends AbstractSimpleFeatureConverterFactory[OsmWay] {

  override protected val typeToProcess = "osm-ways"

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        validating: Boolean): SimpleFeatureConverter[OsmWay] = {
    val pbf = if (conf.hasPath("format")) conf.getString("format").toLowerCase.trim.equals("pbf") else false
    val needsMetadata = fields.exists { case OsmField(_, tag, _) => OsmField.requiresMetadata(tag); case _ => false }
    new OsmWaysConverter(sft, idBuilder, fields, userDataBuilder, validating, pbf, needsMetadata)
  }

  override protected def buildField(field: Config): Field = {
    val name = field.getString("name")
    val transform = if (field.hasPath("transform")) {
      Transformers.parseTransform(field.getString("transform"))
    } else {
      null
    }
    if (field.hasPath("attribute")) {
      OsmField(name, OsmAttribute.withName(field.getString("attribute")), transform)
    } else {
      SimpleField(name, transform)
    }
  }
}