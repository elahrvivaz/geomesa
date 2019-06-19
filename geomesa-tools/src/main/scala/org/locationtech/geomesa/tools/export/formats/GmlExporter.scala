/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io.OutputStream
import java.nio.charset.StandardCharsets

import javax.xml.namespace.QName
import net.opengis.wfs.WfsFactory
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.wfs.WFSConfiguration
import org.geotools.xsd.Encoder
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.{ByteCounter, ByteCounterExporter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GmlExporter private (os: OutputStream, counter: ByteCounter, configuration: WFSConfiguration)
    extends ByteCounterExporter(counter) {

  private val encoder: Encoder = {
    val props = configuration.getProperties.asInstanceOf[java.util.Set[QName]]
    props.add(org.geotools.gml2.GMLConfiguration.OPTIMIZED_ENCODING)
    props.add(org.geotools.gml2.GMLConfiguration.NO_FEATURE_BOUNDS)
    val e = new Encoder(configuration)
    e.getNamespaces.declarePrefix("geomesa", "http://geomesa.org")
    e.setEncoding(StandardCharsets.UTF_8)
    e.setIndenting(true)
    e
  }

  private var sft: SimpleFeatureType = _
  private var retyped: Option[SimpleFeatureType] = _

  override def start(sft: SimpleFeatureType): Unit = {
    this.sft = sft
    this.retyped = if (sft.getName.getNamespaceURI != null) { None } else {
      val builder = new SimpleFeatureTypeBuilder()
      builder.init(sft)
      builder.setNamespaceURI("http://geomesa.org")
      Some(builder.buildFeatureType())
    }
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    // TODO GEOMESA-2648 it would be nice if we could not write a feature collection each time...
    val array = features.toArray
    val collection = {
      val col = WfsFactory.eINSTANCE.createFeatureCollectionType()
      val fc = retyped match {
        case None    => new ListFeatureCollection(sft, array)
        case Some(r) => new ReTypingFeatureCollection(new ListFeatureCollection(sft, array), r)
      }
      col.getFeature.asInstanceOf[java.util.List[SimpleFeatureCollection]].add(fc)
      col
    }

    def encode(): Unit = encoder.encode(collection, org.geotools.wfs.WFS.FeatureCollection, os)

    if (System.getProperty(GmlExporter.TransformerProperty) != null) { encode() } else {
      // explicitly set the default java transformer, to avoid picking up saxon (which causes errors)
      // the default class is hard-coded in javax.xml.transform.TransformerFactory.newInstance() ...
      System.setProperty(GmlExporter.TransformerProperty,
        classOf[com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl].getName)
      try { encode() } finally {
        System.clearProperty(GmlExporter.TransformerProperty)
      }
    }

    os.flush()
    Some(array.length.toLong)
  }

  override def close(): Unit = os.close()
}

object GmlExporter {

  private val TransformerProperty = classOf[javax.xml.transform.TransformerFactory].getName

  def apply(os: OutputStream, counter: ByteCounter): GmlExporter =
    new GmlExporter(os, counter, new org.geotools.wfs.v1_1.WFSConfiguration())

  def gml2(os: OutputStream, counter: ByteCounter): GmlExporter =
    new GmlExporter(os, counter, new org.geotools.wfs.v1_0.WFSConfiguration_1_0())
}
