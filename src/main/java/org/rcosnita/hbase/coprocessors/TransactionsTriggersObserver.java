package org.rcosnita.hbase.coprocessors;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.hbase.TransactionAwareHTable;
import org.apache.tephra.runtime.*;
import org.rcosnita.hbase.proto.Transactions;

import java.io.IOException;

/**
 * TransactionsTriggersObserver provides the logic for executing all the operations encoded in the given request.
 */
public class TransactionsTriggersObserver extends BaseRegionObserver {
    private final static byte[] COLUMN_FAMILY_TRANS = Bytes.toBytes("trans");
    private final static byte[] CELL_NAME_ACC_TRANS = Bytes.toBytes("acc1");

    private final static byte[] COLUMN_FAMILY = Bytes.toBytes("trans");
    private final static byte[] CELL_NAME = Bytes.toBytes("tran");

    private Connection connection;
    private TransactionSystemClient tephraClient;

    public TransactionsTriggersObserver() throws IOException {
        super();

        Configuration conf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);

        Injector injector = Guice.createInjector(
                new ConfigModule(conf),
                new ZKModule(),
                new DiscoveryModules().getDistributedModules(),
                new TransactionModules().getDistributedModules(),
                new TransactionClientModule()
        );

        tephraClient = injector.getInstance(TransactionSystemClient.class);
    }

    private TransactionAwareHTable getTransTable() throws IOException {
        Table table = connection.getTable(TableName.valueOf("transactions"));
        TransactionAwareHTable transTable = new TransactionAwareHTable(table);
        return transTable;
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        Cell c = put.get(COLUMN_FAMILY, CELL_NAME).get(0);
        byte[] tranBytes = new byte[c.getValueLength()];
        System.arraycopy(c.getValueArray(), c.getValueOffset(), tranBytes, 0, c.getValueLength());
        Transactions.Transaction tran = Transactions.Transaction.parseFrom(tranBytes);

        try (TransactionAwareHTable transTable = getTransTable()) {
            TransactionContext context = new TransactionContext(tephraClient, transTable);
            try {
                context.start();

                Put putTran = new Put(Bytes.toBytes("transaction1-glob"));
                putTran.addColumn(COLUMN_FAMILY_TRANS, CELL_NAME_ACC_TRANS, Bytes.toBytes(tran.getValue1()));
                transTable.put(putTran);

                putTran = new Put(Bytes.toBytes("transaction2-glob"));
                putTran.addColumn(COLUMN_FAMILY_TRANS, CELL_NAME_ACC_TRANS, Bytes.toBytes(-tran.getValue1()));
                transTable.put(putTran);

                context.finish();
            } catch (Exception ex) {
                try {
                    context.abort();
                } catch (TransactionFailureException tfex) {
                    System.err.println(tfex);
                    e.complete();
                    return;
                }
            }
        }

        super.prePut(e, put, edit, durability);
    }
}
