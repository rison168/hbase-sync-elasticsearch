package com.rison.hbase;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;


/**
 * @author : Rison 2021/9/15 下午3:29
 * <p>
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
    public ElasticSearchBulkOperator elasticSearchBulkOperator;

    /**
     * read es config from params
     *
     * @param environment
     */
    private void readConfiguration(CoprocessorEnvironment environment) {
        Configuration conf = environment.getConfiguration();
        clusterName = conf.get("es_cluster");
        nodeHost = conf.get("es_host");
        nodePort = conf.getInt("es_port", -1);
        indexName = conf.get("es_index");
        typeName = conf.get("es_type");
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        //read config
        readConfiguration(e);
        //init es client
        client = new ElasticSearchClient(clusterName, nodeHost, nodePort);
        elasticSearchBulkOperator = new ElasticSearchBulkOperator(client);
        client.initElasticsearch();
        LOG.info("===================observer init esClient start===============" + client.getInfo());
    }

    /**
     * stop
     *
     * @param e
     * @throws IOException
     */
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        // close es client
        client.getClient().close();
        // shutdown time task
        elasticSearchBulkOperator.shutdownScheduleEx();
    }

    /**
     * Called after the client stores a value
     * after data put to hbase then prepare update builder to bulk  ES
     *
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(put.getRow());
        try {
            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            // Map<String, Object> infoJson = new HashMap<String, Object>();
            Map<String, Object> json = new HashMap<String, Object>();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                for (Cell cell : entry.getValue()) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    //处理时间格式,将使其能够自动转换为时间格式
                    if ("date".equals(key)) {
                        value = value.replace(" ", "T") + "+0800";
                    }
                    json.put(key, value);
                }
            }
            // set hbase family to es
            //infoJson.put("info", json);
            elasticSearchBulkOperator.addUpdateBuilderToBulk(client.getClient().prepareUpdate(indexName, typeName, indexId).setDocAsUpsert(true).setDoc(json));
        } catch (Exception ex) {
            LOG.error("observer put  a doc, index [ " + client.getClusterName() + " ]" + "indexId [" + indexId + "] error : " + ex.getMessage());
        }
    }


    /**
     * Called after the client deletes a value.
     * after data delete from hbase then prepare delete builder to bulk  ES
     *
     * @param e
     * @param delete
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(delete.getRow());
        try {
            elasticSearchBulkOperator.addDeleteBuilderToBulk(client.getClient().prepareDelete(indexName, typeName, indexId));
        } catch (Exception ex) {
            LOG.error(ex);
            LOG.error("observer delete  a doc, index [ " + client.getClusterName() + " ]" + "indexId [" + indexId + "] error : " + ex.getMessage());

        }
    }


}
