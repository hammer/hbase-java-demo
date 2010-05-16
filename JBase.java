import java.io.IOException;

import java.util.Arrays;
import java.util.Collection;
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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;

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
	System.out.println("HTableDescriptor version: " + HTableDescriptor.TABLE_DESCRIPTOR_VERSION);
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

	// TODO(hammer): use commons.io to get human-readable byte counts
	// TABLES
	Path hbaseRootDir = new Path(config.get("hbase.rootdir"));
	HTableDescriptor[] tables = admin.listTables();
	HTableDescriptor[] allTables = Arrays.copyOf(tables, tables.length + 2);
	allTables[allTables.length - 2] = HTableDescriptor.META_TABLEDESC;
	allTables[allTables.length - 1] = HTableDescriptor.ROOT_TABLEDESC;
	for (HTableDescriptor t : Arrays.asList(allTables)) {
	    System.out.println("Information about table " + t.getNameAsString());
	    System.out.println("  Stored at " + t.getTableDir(hbaseRootDir, t.getName()));
	    System.out.println("  MemStore flush size: " + t.getMemStoreFlushSize());
	    System.out.println("  Maximum store file size: " + t.getMaxFileSize());
	    Collection<HColumnDescriptor> cfs = t.getFamilies();
	    System.out.println("  Number of Column Families: " + cfs.size());

	    // COLUMN FAMILIES
	    for (HColumnDescriptor c : cfs) {
		System.out.println("  Information about column family " + c.getNameAsString());
		System.out.println("    HDFS block size: " + c.getBlocksize());
		System.out.println("    Compression: " + c.getCompression());
		System.out.println("    Max Versions: " + c.getMaxVersions());
		System.out.println("    TTL: " + c.getTimeToLive());
		System.out.println("    Block Cache: " + c.isBlockCacheEnabled());
		System.out.println("    Bloom Filters: " + c.isBloomfilter());
		System.out.println("    In Memory: " + c.isInMemory());
	    }

	    // REGIONS
	    // Would use HTable.getRegionsInfo() to get this information,
	    // but trying to use only HBaseAdmin here
	}

	// SPLITS
	//	admin.split("t1");

	// COMPACTIONS
	//	admin.flush("t1"); // send memtable to disk; async
	//	admin.compact("t1"); // best effort aggregation of some store files and removal of deleted cells
	//	admin.majorCompact("t1"); // complete aggregation of all store files and removal of all deleted cells

	// USER
	HTable table = new HTable(config, "t1");
	System.out.println("\n\nConnected to table: " + Bytes.toString(table.getTableName()));

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
