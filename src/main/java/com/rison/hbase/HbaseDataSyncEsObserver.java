package com.rison.hbase;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;

/**
 * @author : Rison 2021/9/15 下午3:29
 *
 * Hbase Sync data to ES
 */
public class HbaseDataSyncEsObserver extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(HbaseDataSyncEsObserver.class);

    public String clusterName;
    public String nodeHost;
    public String indexName;
    public String typeName;
    public Integer nodePort;
    public ElasticSearchClient client;
}
