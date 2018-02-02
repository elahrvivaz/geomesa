/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeatureType;

public interface Metadata {
    String getEncoding();
    PartitionScheme getPartitionScheme();
    SimpleFeatureType getSimpleFeatureType();

    int getPartitionCount();
    int getFileCount();

    void addFile(String partition, String file);
    void addFiles(String partition, java.util.List<String> files);
    void addFiles(java.util.Map<String, java.util.List<String>> partitionsToFiles);

    void removeFile(String partition, String file);
    void removeFiles(String partition, java.util.List<String> files);
    void removeFiles(java.util.Map<String, java.util.List<String>> partitionsToFiles);

    void replaceFiles(String partition, java.util.List<String> files, String replacement);

    void setFiles(java.util.Map<String, java.util.List<String>> partitionsToFiles);

    java.util.List<String> getPartitions();
    java.util.List<String> getFiles(String partition);
    java.util.Map<String, java.util.List<String>> getPartitionFiles();
}
