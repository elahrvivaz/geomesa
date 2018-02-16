Using the FileSystem Data Store Programmatically
================================================

Creating a Data Store
---------------------

An instance of a FileSystem data store can be obtained through the normal GeoTools discovery methods, assuming that
the GeoMesa code is on the classpath:

.. code-block:: java

    Map<String, String> parameters = new HashMap<>;
    parameters.put("fs.path", "hdfs://localhost:9000/fs-root/");
    parameters.put("fs.encoding", "parquet");
    org.geotools.data.DataStore dataStore = org.geotools.data.DataStoreFinder.getDataStore(parameters);

More information on using GeoTools can be found in the `GeoTools user guide <http://docs.geotools.org/stable/userguide/>`_.

.. _fsds_parameters:

FileSystem Data Store Parameters
--------------------------------

The FileSystem data store takes several parameters (required parameters are marked with ``*``):

=================== ====== ===========================================================================================================
Parameter           Type   Description
=================== ====== ===========================================================================================================
``fs.path *``       String The root path to write and read data from (e.g. s3a://mybucket/datastores/testds)
``fs.encoding *``   String The encoding of the stored files. Provided implementations are "parquet" and "orc".
``fs.read-threads`` Int    The number of threads used for queries
``fs.config``       String Configuration values, formatted as a Java properties file, that will be passed to the underlying FileSystem
=================== ====== ===========================================================================================================
