package org.rcosnita.hbase.coprocessors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.rcosnita.hbase.proto.Transactions;

import java.io.IOException;

public class TransactionsTriggersObserverTests {
    private Table transTable;

    @Before
    public void setup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String path = this.getClass()
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        conf.addResource(new Path(path));
        HBaseAdmin.checkHBaseAvailable(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        transTable = connection.getTable(TableName.valueOf("transactions_triggers"));
    }

    @Test
    public void testPutTransactionDecodingWorks() throws IOException {
        Transactions.Transaction tran = Transactions.Transaction.newBuilder()
                .setAcc1("works as expected")
                .setValue1(150)
                .build();

        Put put = new Put(Bytes.toBytes("tran1"));
        put.addColumn(Bytes.toBytes("trans"), Bytes.toBytes("tran"), tran.toByteArray());
        transTable.put(put);
    }
}
