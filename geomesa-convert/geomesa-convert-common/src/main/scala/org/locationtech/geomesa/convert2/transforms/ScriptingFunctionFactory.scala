/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.commons.io.{FileUtils, FilenameUtils, IOUtils}
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{File, FileReader, InputStream, InputStreamReader, Reader}
import java.net.{JarURLConnection, URI, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.FileSystems
import java.util.Collections
import java.util.zip.ZipInputStream
import javax.script.{Invocable, ScriptContext, ScriptEngine, ScriptEngineManager}
import scala.collection.JavaConverters._
import scala.io.{Codec, Source}
import java.nio.file.Files

/**
  * Provides TransformerFunctions that execute javax.scripts compatible functions defined
  * on the classpath or in external directories.  Scripting languages are determined by
  * the extension of the file.  For instance, 'name.js' will be interpreted as a javascript file.
  */
class ScriptingFunctionFactory extends TransformerFunctionFactory with LazyLogging {

  import ScriptingFunctionFactory._

  override lazy val functions: Seq[TransformerFunction] = {
    val loader = new ScriptLoader()
    loader.loadScripts()
    val functions = loader.functions
    logger.debug("functions: " + functions.map(_.names.mkString("|")).mkString(","))
    functions
  }
}

object ScriptingFunctionFactory extends LazyLogging {

  val ConvertScriptsPathProperty = "geomesa.convert.scripts.path"
  val ConvertScriptsClassPath    = "geomesa-convert-scripts"

  val ConvertScriptsPath: SystemProperty = SystemProperty(ConvertScriptsPathProperty)

  /**
   * Manages loading scripts into a script engine
   */
  private class ScriptLoader {

    private val manager = new ScriptEngineManager()
    private val engines = new java.util.HashMap[String, ScriptEngine]()

    def evaluateScript(file: File): Unit =
      evaluateScript(new FileReader(file), FilenameUtils.getExtension(file.getName))

    def evaluateScript(reader: Reader, extension: String): Unit = {
      logger.debug(s"extension: '${extension}'")
      if (!engines.containsKey(extension)) {
        val m = manager.getEngineByExtension(extension)
        logger.debug("" + m)
        engines.put(extension, m)
      }
      val engine = engines.get(extension)
      if (engine != null) {
        val text = IOUtils.toString(reader)
        logger.debug(text)
        engine.eval(text)
      }
    }

    def functions: Seq[TransformerFunction] = {
      engines.asScala.toList.filter(_._2 != null).flatMap { case (ext, engine) =>
        logger.debug(s"Ext $ext engine $engine")
        engine.getBindings(ScriptContext.ENGINE_SCOPE).asScala.keys.map { name =>
          new ScriptTransformerFn(ext, name, engine.asInstanceOf[Invocable])
        }
      }
    }

    def loadScripts(): Unit = {
      logger.debug("Loading scripts...")
      loadScriptsFromEnvironment()
      loadScriptsFromClasspath(Thread.currentThread().getContextClassLoader)
    }

    /**
     * <p>Load scripts from the environment using the property "geomesa.convert.scripts.path"
     * Entries are colon (:) separated. Entries can be files or directories. Directories will
     * be recursively searched for script files. The extension of script files defines what
     * kind of script they are (e.g. js = javascript)</p> */
    def loadScriptsFromEnvironment(): Unit = {
      val files = ConvertScriptsPath.option.toSeq.flatMap(_.split(":")).flatMap { path =>
        val file = new File(path)
        if (!file.exists()) {
          logger.warn(s"Ignoring non-existent file: ${file.getAbsolutePath}")
          Seq.empty
        } else if (!file.canRead) {
          logger.warn(s"Ignoring non-readable file: ${file.getAbsolutePath}")
          Seq.empty
        } else if (file.isDirectory) {
          if (file.canExecute) {
            FileUtils.listFiles(file, TrueFileFilter.TRUE, TrueFileFilter.TRUE).asScala
          } else {
            logger.warn(s"Ignoring non-executable dir: ${file.getAbsolutePath}")
            Seq.empty
          }
        } else {
          Seq(file)
        }
      }

      files.foreach { f =>
        logger.debug(s"Evaluating script at: ${f.getAbsolutePath}")
        evaluateScript(f)
      }
    }

    /**
      * Load scripts from the resource geomesa-convert-scripts from a classloader. To use this
      * create a folder in the jar named "geomesa-convert-scripts" and place script files
      * within that directory.
      *
      * @param cl classloader used to load resources
      * @return
      */
    def loadScriptsFromClasspath(cl: ClassLoader): Unit = {
      cl.getResources(ConvertScriptsClassPath + "/").asScala.foreach { url =>
        val uri = url.toURI
        uri.getScheme match {
          case "jar" =>
            // indicates a jar with a scripts directory inside it
            logger.debug(s"Reading jar at: $url")
            WithClose(FileSystems.newFileSystem(uri, Collections.emptyMap[String, AnyRef](), cl)) { fs =>
              WithClose(Files.walk(fs.getPath(ConvertScriptsClassPath + "/"))) { scripts =>
                scripts.iterator.asScala.foreach { script =>
                  val scriptUri = script.toUri
                  // note: make sure to use the schemeSpecificPart to get an accurate extension
                  val ext = FilenameUtils.getExtension(scriptUri.getSchemeSpecificPart)
                  if (ext.nonEmpty) {
                    WithClose(scriptUri.toURL.openStream()) { is =>
                      logger.debug(s"Evaluating script at: $scriptUri")
                      evaluateScript(new InputStreamReader(is, StandardCharsets.UTF_8), ext)
                    }
                  }
                }
              }
            }

          case "file" =>
            // indicates a directory on the classpath
            // reading the uri will list the directory contents
            logger.debug(s"Reading directory at: $url")
            WithClose(Source.fromURI(uri)(Codec.UTF8)) { source =>
              source.getLines().foreach { script =>
                val url = cl.getResource(s"$ConvertScriptsClassPath/$script")
                // note: make sure to use the schemeSpecificPart to get an accurate extension
                val ext = FilenameUtils.getExtension(url.toURI.getSchemeSpecificPart)
                WithClose(url.openStream()) { is =>
                  logger.debug(s"Evaluating script at: $url")
                  evaluateScript(new InputStreamReader(is, StandardCharsets.UTF_8), ext)
                }
              }
            }

          case s =>
            logger.warn(s"Ignoring script resource of type: $s")
            Seq.empty
        }
      }
    }
  }

  class ScriptTransformerFn(ext: String, name: String, engine: Invocable)
      extends NamedTransformerFunction(Seq(s"$ext:$name")) {
    override def apply(args: Array[AnyRef]): AnyRef = engine.invokeFunction(name, args: _*)
  }
}

