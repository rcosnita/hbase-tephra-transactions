package org.rcosnita.hbase.coprocessors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * RegionObserverExample provides a simple coprocessors which rejects unauthorized requests to admin details.
 */
public class RegionObserverExample extends BaseRegionObserver {
    private static final byte[] ADMIN = Bytes.toBytes("admin");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("details");
    private static final byte[] COLUMN = Bytes.toBytes("Admin_det");
    private static final byte[] VALUE = Bytes.toBytes("You can't see admin details 2");

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e, final Get get, final List<Cell> results) throws IOException {
        if (Bytes.equals(get.getRow(), ADMIN)) {
            Cell c = CellUtil.createCell(get.getRow(), COLUMN_FAMILY, COLUMN, System.currentTimeMillis(), (byte)4, VALUE);
            results.add(c);
            e.bypass();
            return;
        }

        super.preGetOp(e, get, results);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        e.bypass();
    }
}