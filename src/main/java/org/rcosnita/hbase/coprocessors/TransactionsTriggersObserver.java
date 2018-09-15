package org.rcosnita.hbase.coprocessors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.rcosnita.hbase.proto.Transactions;

import java.io.IOException;

/**
 * TransactionsTriggersObserver provides the logic for executing all the operations encoded in the given request.
 */
public class TransactionsTriggersObserver extends BaseRegionObserver {
    private final static byte[] COLUMN_FAMILY = Bytes.toBytes("trans");
    private final static byte[] CELL_NAME = Bytes.toBytes("tran");

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        Cell c = put.get(COLUMN_FAMILY, CELL_NAME).get(0);
        byte[] tranBytes = new byte[c.getValueLength()];
        System.arraycopy(c.getValueArray(), c.getValueOffset(), tranBytes, 0, c.getValueLength());

        Transactions.Transaction tran = Transactions.Transaction.parseFrom(tranBytes);
        put.getFamilyCellMap().clear();
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("account"), Bytes.toBytes(tran.getAcc1()));
        put.addColumn(COLUMN_FAMILY, Bytes.toBytes("value"), Bytes.toBytes(tran.getValue1()));

        super.prePut(e, put, edit, durability);
    }
}
