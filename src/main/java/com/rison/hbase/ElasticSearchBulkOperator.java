package com.rison.hbase;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author : Rison 2021/9/15 下午3:32
 * Bulk hbase data to ElasticSearch
 */
public class ElasticSearchBulkOperator {
    private static final Log LOG = LogFactory.getLog(ElasticSearchBulkOperator.class);

    /**
     * 此处设置批量提交数量
     */
    private static final int MAX_BULK_COUNT = 5000;

    private BulkRequestBuilder bulkRequestBuilder = null;

    private Lock commitLock = new ReentrantLock();

    private ScheduledExecutorService scheduledExecutorService = null;

    private ElasticSearchClient elasticSearchClient = null;

    public ElasticSearchBulkOperator(final ElasticSearchClient elasticSearchClient) {
        LOG.info("----------------- Init Bulk Operator for Table: " + " ----------------");
        this.elasticSearchClient = elasticSearchClient;
        //init es bulkRequestBuilder
        this.bulkRequestBuilder = elasticSearchClient.getClient().prepareBulk();
        //init thread pool and set size 1;

        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.
                Builder().namingPattern("esClient-schedule-pool-%d").daemon(true).build());

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                commitLock.lock();
                try {
                    bulkRequest(0);
                } catch (Exception e) {
                    e.getMessage();
                    LOG.error("Time Bulk " + " index error : " + e.getMessage());
                } finally {
                    commitLock.unlock();
                }

            }
        };
        // set runnable thread(15 second to delay first execution , 25 second period between successive executions)
        this.scheduledExecutorService.scheduleAtFixedRate(runnable, 15, 25, TimeUnit.SECONDS);
    }

    /**
     * shutdown time task
     */
    public void shutdownScheduleEx() {
        if (null != scheduledExecutorService && !scheduledExecutorService.isShutdown()) {
            scheduledExecutorService.shutdown();
        }
    }

    /**
     * bulk request when number of builders is grate then threshold
     *
     * @param threshold
     */
    public void bulkRequest(int threshold) {
        int count = bulkRequestBuilder.numberOfActions();
        if (bulkRequestBuilder.numberOfActions() > threshold) {
            BulkResponse bulkItemResponses = bulkRequestBuilder.execute().actionGet();
            if (bulkItemResponses.hasFailures()) {
                bulkItemResponses.buildFailureMessage();
            }
            bulkRequestBuilder = elasticSearchClient.getClient().prepareBulk();

            List<DocWriteRequest> docWriteRequestList = bulkRequestBuilder.request().requests();
            elasticSearchClient.getClient().close();
            elasticSearchClient.repeatInitEsClient();
            bulkRequestBuilder = elasticSearchClient.getClient().prepareBulk();
            bulkRequestBuilder.request().add(docWriteRequestList);
        }
    }

    /**
     * add update builder to bulk use commitLock to protected bulk as
     * thread-save
     *
     * @param builder
     */
    public void addUpdateBuilderToBulk(UpdateRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception e) {
            e.getMessage();
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * add delete builder to bulk use commitLock to protected bulk as
     * thread-save
     *
     * @param builder
     */
    public void addDeleteBuilderToBulk(DeleteRequestBuilder builder) {
        commitLock.lock();
        try {
            bulkRequestBuilder.add(builder);
            bulkRequest(MAX_BULK_COUNT);
        } catch (Exception e) {
            e.getMessage();
        } finally {
            commitLock.unlock();
        }
    }
}
