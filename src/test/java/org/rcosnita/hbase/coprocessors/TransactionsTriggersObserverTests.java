package org.rcosnita.hbase.coprocessors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rcosnita.hbase.proto.Transactions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TransactionsTriggersObserverTests {
    volatile Connection connection;

    @Before
    public void setup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        conf.addResource(new Path(path));
        HBaseAdmin.checkHBaseAvailable(conf);
        connection = ConnectionFactory.createConnection(conf);
    }

    @Test
    public void testPutTransactionTephraEnabled() throws InterruptedException, IOException, ExecutionException {
        final int ITERATIONS = 40;
        testWriteWithExecutorService(ITERATIONS);

        Get get = new Get(Bytes.toBytes("transaction1-glob"));
        Result result = getTransDetailsTable().get(get);
        Assert.assertNotNull(result);

        byte[] tranBytes = result.getValue(Bytes.toBytes("trans"), Bytes.toBytes("acc1"));
        long value = Bytes.toLong(tranBytes);
        System.out.println(value);
    }

    private Table getTransTable() throws IOException {
        return connection.getTable(TableName.valueOf("transactions_triggers"));
    }

    private Table getTransDetailsTable() throws IOException {
        return connection.getTable(TableName.valueOf("transactions"));
    }

    private void testWriteWithExecutorService(int numOfIterations) throws InterruptedException, ExecutionException {
        ExecutorService executor = new ForkJoinPool(Runtime.getRuntime().availableProcessors() * 2);
        List<Future<?>> pendingFutures = new ArrayList<Future<?>>();

        for (int i = 0; i < numOfIterations; i++) {
             writeRandomTransaction(i + 1, executor, pendingFutures);
        }

        for (Future<?> f : pendingFutures) {
            f.get();
        }
    }

    private void writeRandomTransaction(final int seed, ExecutorService executor, List<Future<?>> pendingFutures) {
        pendingFutures.add(executor.submit(new Runnable() {
            @Override
            public void run() {
                Transactions.Transaction tran = Transactions.Transaction.newBuilder()
                        .setAcc1("works as expected")
                        .setValue1(150 * seed)
                        .build();

                Put put = new Put(Bytes.toBytes("tran:" + seed));
                put.addColumn(Bytes.toBytes("trans"), Bytes.toBytes("tran"), tran.toByteArray());

                try (Table table = getTransTable()) {
                    table.put(put);
                } catch (Exception ex) {
                    System.err.println(ex);
                }
            }
        }));
    }
}
