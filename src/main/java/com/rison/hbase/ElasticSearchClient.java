package com.rison.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static java.net.InetAddress.getByName;

/**
 * @author : Rison 2021/9/15 下午2:48
 * <p>
 * elasticsearch client
 */
public class ElasticSearchClient {

    @PostConstruct
    private void init() {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
    }

    /**
     * elasticsearch 的集群名称
     */
    private String clusterName;
    /**
     * elasticsearch 的host
     */
    private String nodeHost;
    /**
     * elasticsearch的端口（java API 用的是Transport 端口， 也就是TCP）
     */
    private int nodePort;
    /**
     * TransportClient
     */
    private TransportClient client = null;

    private static final Log LOG = LogFactory.getLog(ElasticSearchClient.class);

    /**
     * 获取es 配置信息
     *
     * @param clusterName
     * @param nodeHost
     * @param nodePort
     */
    public ElasticSearchClient(String clusterName, String nodeHost, int nodePort) {
        this.clusterName = clusterName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.client = initElasticsearch();
    }

    /**
     * 初始化 es client
     *
     * @return
     */
    private TransportClient initElasticsearch() {
        LOG.info("====================正在初始化：" + this.clusterName + "=========================");
        TransportClient client = null;
        Settings settings = Settings.builder()
                .put("cluster.name", this.clusterName)
                .put("client.transport.sniff", true)
                .build();

        try {
            client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName(this.nodeHost), this.nodePort));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            LOG.error("========================初始化es client 失败" + this.clusterName + "==============================");
        }
        LOG.info("========================初始化es client 结束" + this.clusterName + "==============================");
        return client;
    }

    /**
     * get es info
     *
     * @return
     */
    public String getInfo() {
        List<String> fields = new ArrayList<>();
        for (Field field : ElasticSearchClient.class.getDeclaredFields()) {
            try {
                fields.add(field.getName() + "=" + field.get(this));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return StringUtils.join(fields, ",");
    }

    public void repeatInitEsClient() {
        this.client = initElasticsearch();
    }

    /**
     * @return the clusterName
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * @param clusterName the clusterName to set
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * @return the nodePort
     */
    public int getNodePort() {
        return nodePort;
    }

    /**
     * @param nodePort the nodePort to set
     */
    public void setNodePort(int nodePort) {
        this.nodePort = nodePort;
    }

    /**
     * @return the client
     */
    public TransportClient getClient() {
        return client;
    }

    /**
     * @param client the client to set
     */
    public void setClient(TransportClient client) {
        this.client = client;
    }

}
