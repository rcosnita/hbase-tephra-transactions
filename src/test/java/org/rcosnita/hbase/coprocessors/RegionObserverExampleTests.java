package org.rcosnita.hbase.coprocessors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class RegionObserverExampleTests {
    private Table usersTable;

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
        usersTable = connection.getTable(TableName.valueOf("users"));
    }

    @Test
    public void testGetUnauthorizedAdminDetails() throws IOException {
        Get get = new Get(Bytes.toBytes("admin"));
        Result result = usersTable.get(get);
        Cell c = result.getColumnLatestCell(Bytes.toBytes("details"), Bytes.toBytes("Admin_det"));
        Assert.assertNotNull(c);

        byte[] value = new byte[c.getValueLength()];
        System.arraycopy(c.getValueArray(), c.getValueOffset(), value, 0, c.getValueLength());

        Assert.assertEquals("You can't see admin details 2", Bytes.toString(value));
    }
}
