// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.metadata

import com.yahoo.bard.webservice.application.JerseyTestBinder

import com.yahoo.bard.webservice.table.PhysicalTableDictionary

import java.util.concurrent.atomic.AtomicReference

class DataSourceMetadataServiceSpec extends BaseDataSourceMetadataSpec {

    def "test metadata service updates segment availability for physical tables"() {
        setup:
        JerseyTestBinder jtb = new JerseyTestBinder()
        PhysicalTableDictionary tableDict = jtb.configurationLoader.getPhysicalTableDictionary()
        DataSourceMetadataService metadataService = new DataSourceMetadataService()
        DataSourceMetadata metadata = new DataSourceMetadata(tableName, [:], segments)

        when:
        metadataService.update(tableDict.get(tableName), metadata)

        then:
        metadataService.allSegmentsByTime.get(tableDict.get(tableName).getTableName()) instanceof AtomicReference
        metadataService.allSegmentsByColumn.get(tableDict.get(tableName).getTableName()) instanceof AtomicReference

        and:
        metadataService.allSegmentsByTime.get(tableDict.get(tableName).getTableName()).get().values()*.keySet() as List ==
        [
                [segment1.getIdentifier(), segment2.getIdentifier()] as Set,
                [segment3.getIdentifier(), segment4.getIdentifier()] as Set
        ] as List

        and:
        // all the intervals in meatadataService are interval12
        [interval12].containsAll(metadataService.allSegmentsByColumn.get(tableDict.get(tableName).getTableName()).get().values().toSet().getAt(0))

        cleanup:
        jtb.tearDown()
    }
}
