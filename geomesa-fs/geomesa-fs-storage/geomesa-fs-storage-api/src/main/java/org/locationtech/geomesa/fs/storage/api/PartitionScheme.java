/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface PartitionScheme {

    /**
     * Name of this partition scheme
     *
     * @return name
     */
    String getName();

    /**
     * Return the partition in which a SimpleFeature should be stored
     *
     * @param feature simple feature
     * @return partition name
     */
    String getPartition(SimpleFeature feature);

    /**
     * Return a list of partitions that the system needs to query
     * in order to satisfy a filter predicate
     *
     * Note that if the filter does not constrain the partitions at
     * all, an empty list will be returned. This can't be disambiguated
     * with a filter that doesn't match any partitions - instead, use
     * `getPartitionsForQuery`
     *
     * @param filter filter
     * @return list of partitions that may have results from the filter
     * @deprecated use getPartitionsForQuery
     */
    @Deprecated
    List<String> getPartitions(Filter filter);

    /**
     * Return a list of modified filters and partitions. Each filter will have been simplified to
     * remove any predicates that are implicitly true for the associated partitions
     *
     * If the filter does not constrain partitions at all, then an empty option will be returned,
     * indicating all partitions much be searched. If the filter excludes all potential partitions,
     * then an empty list of partitions will be returned
     *
     * @param filter filter
     * @return list of simplified filters and partitions
     */
    default Optional<List<FilterPartitions>> getPartitionsForQuery(Filter filter) {
        return getPartitionsForQuery(filter, true);
    }

    /**
     * Return a list of modified filters and partitions. If `simplify` is true, each filter will have
     * been simplified to remove any predicates that are implicitly true for the associated partitions,
     * otherwise all intersecting partitions will be returned in one group
     *
     * If the filter does not constrain partitions at all, then an empty option will be returned,
     * indicating all partitions much be searched. If the filter excludes all potential partitions,
     * then an empty list of partitions will be returned
     *
     * @param filter filter
     * @param simplify simplify filters based on intersecting/covered partitions
     * @return list of simplified filters and partitions
     */
    default Optional<List<FilterPartitions>> getPartitionsForQuery(Filter filter, boolean simplify) {
        List<String> partitions = getPartitions(filter);
        // default implementation does no optimization
        if (partitions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(Collections.singletonList(new FilterPartitions(filter, partitions)));
        }
    }

    /**
     *
     * @return the max depth this partition scheme goes to
     */
    int getMaxDepth();

    /**
     * Are partitions stored as leaves (multiple partitions in a single folder), or does each
     * partition have a unique folder. Using leaf storage can reduce the level of nesting and make
     * file system operations faster in some cases.
     *
     * @return leaf
     */
    boolean isLeafStorage();

    /**
     * Options used to configure this scheme - @see PartitionSchemeFactory
     *
     * @return options
     */
    Map<String, String> getOptions();
}
