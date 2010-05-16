import java.io.IOException;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.HBaseConfiguration;

// HBaseAdmin
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;

// HTable
import org.apache.hadoop.hbase.client.HTable;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

public class JBase {
    public static void main(String[] args) throws IOException {
	// CONFIGURATION
	HBaseConfiguration config = new HBaseConfiguration();

	// ENSURE RUNNING
	try {
	    HBaseAdmin.checkHBaseAvailable(config);
	} catch (MasterNotRunningException e) {
	    System.out.println("HBase is not running!");
	    System.exit(1);
	}
	System.out.println("HBase is running!");

	// ADMIN
	HBaseAdmin admin = new HBaseAdmin(config);

	// CLUSTER STATUS
	ClusterStatus cluster = admin.getClusterStatus();
	System.out.println("HBase version: " + cluster.getHBaseVersion());
	System.out.println("Number of servers: " + cluster.getServers());
	System.out.println("Number of regions: " + cluster.getRegionsCount());

	// SERVER LOAD
	for (HServerInfo si : cluster.getServerInfo()) {
	    System.out.println("Server statistics for " + si.getServerName());
	    HServerLoad sl = si.getLoad();
	    System.out.println("  Load: " + sl);
	    for (RegionLoad rl : sl.getRegionsLoad()) {
		System.out.println("  Region " + rl.getNameAsString() + " Load: " + rl);
	    }
	}

	// USER
	HTable table = new HTable(config, "t1");
	System.out.println("Connected to table: " + Bytes.toString(table.getTableName()));

	// PUT
	Put p = new Put(Bytes.toBytes("r1"));
	p.add(Bytes.toBytes("cf1"), Bytes.toBytes("c3"), Bytes.toBytes("fromtheclient"));
	table.put(p);

	// GET
	Get g = new Get(Bytes.toBytes("r1"));
	Result r = table.get(g);
	byte [] value = r.getValue(Bytes.toBytes("cf1"),
				   Bytes.toBytes("c3"));
	String valueStr = Bytes.toString(value);
	System.out.println("Get r1 cf1:c3: " + valueStr);

	// SCAN
	Scan s = new Scan();
	s.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c1"));
	ResultScanner scanner = table.getScanner(s);
	try {
	    for (Result rr : scanner) {
	        System.out.println("Scan found row: " + rr);
	    }
	} finally {
	    scanner.close();
	}

        // DELETE
	Put p2 = new Put(Bytes.toBytes("lame duck"));
	p2.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"),
               Bytes.toBytes("not for long"));
	table.put(p2);
	Get g2 = new Get(Bytes.toBytes("lame duck"));
	Result r2 = table.get(g2);
	byte [] value2 = r2.getValue(Bytes.toBytes("cf1"),
         			   Bytes.toBytes("c1"));
	String valueStr2 = Bytes.toString(value2);
	System.out.println("Row we're going to delete: " + valueStr2);
	Delete d = new Delete(Bytes.toBytes("lame duck"));
	table.delete(d);
	Get g3 = new Get(Bytes.toBytes("lame duck"));
	Result r3 = table.get(g3);
	byte [] value3 = r3.getValue(Bytes.toBytes("cf1"),
                                    Bytes.toBytes("c1"));
	String valueStr3 = Bytes.toString(value3);
	System.out.println("Row value after deletion: " + valueStr3);

        // FILTERS
        Filter f1 = new PrefixFilter(Bytes.toBytes("r"));
        Filter f2 = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("c2")));
        List<Filter> fs = Arrays.asList(f1, f2);
        Filter f3 = new FilterList(Operator.MUST_PASS_ALL, fs);
        Scan s2 = new Scan();
        s2.setFilter(f3); // could also do f1 or f2 to show that those work too
	ResultScanner scanner2 = table.getScanner(s2);
        try {
	    // TODO(hammer): Figure out how to write a Writable to System.out
	    for (Result rr : scanner2) {
		System.out.println("Filter " + s2.getFilter() +  " matched row: " + rr);
	    }
	} finally {
	    scanner2.close();
	}

	// DISCONNECT
	table.close();
    }
}
