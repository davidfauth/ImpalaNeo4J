package impalaNeo4JImporter;


import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

import java.io.*;
import java.util.*;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProviderNewImpl;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

import static org.neo4j.helpers.collection.MapUtil.map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class impalaNeo4JImporter {
    private static Report report;
    private BatchInserter db;
    private BatchInserterIndexProvider lucene;
 	
    public static final File STORE_DIR = new File("/Users/davidfauth/graphDBIndexes");
        
    public static final int USERS = 50000000;
    public static final int PROVIDERS = 50000000;
       
	private static final String SQL_STATEMENT = "SELECT * from organizations";
	
	// set the impalad host
	private static final String IMPALAD_HOST = "192.168.1.8";
	
	// port 21050 is the default impalad JDBC port 
	private static final String IMPALAD_JDBC_PORT = "21050";

	private static final String CONNECTION_URL = "jdbc:hive2://" + IMPALAD_HOST + ':' + IMPALAD_JDBC_PORT + "/;auth=noSasl";

	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	enum MyRelationshipTypes implements RelationshipType {PRESCRIBES,ZIP_LOCATION, PARENT_OF, LOCATED_IN, REFERRED, SPECIALTY, SUPPORTS, FOR, CONTRIBUTES, RECEIVES, GAVE,COUNTY_OF, INCOME_IN}
   	Map<String,Long> cache = new HashMap<String,Long>(USERS);
    Map<String,Long> providers = new HashMap<String,Long>(PROVIDERS);
    
    public impalaNeo4JImporter(File graphDb) {
    	Map<String, String> config = new HashMap<String, String>();
    	try {
	        if (new File("batch.properties").exists()) {
	        	System.out.println("Using Existing Configuration File");
	        } else {
		        System.out.println("Writing Configuration File to batch.properties");
				FileWriter fw = new FileWriter( "batch.properties" );
                fw.append( "use_memory_mapped_buffers=true\n"
                        + "neostore.nodestore.db.mapped_memory=100M\n"
                        + "neostore.relationshipstore.db.mapped_memory=2G\n"
                        + "neostore.propertystore.db.mapped_memory=1G\n"
                        + "neostore.propertystore.db.strings.mapped_memory=200M\n"
		                 + "neostore.propertystore.db.arrays.mapped_memory=0M\n"
		                 + "neostore.propertystore.db.index.keys.mapped_memory=15M\n"
		                 + "neostore.propertystore.db.index.mapped_memory=15M" );
		        fw.close();
	        }
	        
        config = MapUtil.load( new File(
                "batch.properties" ) );

        } catch (Exception e) {
    		System.out.println(e.getMessage());
        }
                
        db = createBatchInserter(graphDb, config);
        lucene = createIndexProvider();
        report = createReport();
    }

    protected StdOutReport createReport() {
        return new StdOutReport(10 * 1000 * 1000, 100);
    }

    protected LuceneBatchInserterIndexProviderNewImpl createIndexProvider() {
        return new LuceneBatchInserterIndexProviderNewImpl(db);
    }

    protected BatchInserter createBatchInserter(File graphDb, Map<String, String> config) {
        return BatchInserters.inserter(graphDb.getAbsolutePath(), config);
    }

    public static void main(String[] args) throws IOException {
    	   
        File graphDb = STORE_DIR;

        
        if (graphDb.exists()) {
            FileUtils.deleteRecursively(graphDb);
        }
 
        impalaNeo4JImporter importBatch = new impalaNeo4JImporter(graphDb);
        long startTime = System.nanoTime();

        importBatch.importNames("temp");
        System.out.println("Finished");
        long endTime = System.nanoTime();
        System.out.println("Total time: " + (double)(endTime - startTime)/1000000000.0);
    }

    void finish() {
        lucene.shutdown();
        db.shutdown();
       	System.out.println("Shutdown Finished");
   }

    public static class Data {
        private Object[] data;
        private final int offset;
        private final String delim;
        private final String[] fields;
        private final String[] lineData;
        private final Type types[];
        private final int lineSize;
        private int dataSize;

        public Data(String header, String delim, int offset) {
            this.offset = offset;
            this.delim = delim;
            fields = header.split(delim);
            lineSize = fields.length;
            types = parseTypes(fields);
            lineData = new String[lineSize];
            createMapData(lineSize, offset);
        }

        private Object[] createMapData(int lineSize, int offset) {
            dataSize = lineSize - offset;
            data = new Object[dataSize*2];
            for (int i = 0; i < dataSize; i++) {
                data[i * 2] = fields[i + offset];
            }
            return data;
        }

        private Type[] parseTypes(String[] fields) {
            Type[] types = new Type[lineSize];
            Arrays.fill(types, Type.STRING);
            for (int i = 0; i < lineSize; i++) {
                String field = fields[i];
                int idx = field.indexOf(':');
                if (idx!=-1) {
                   fields[i]=field.substring(0,idx);
                   types[i]= Type.fromString(field.substring(idx + 1));
                }
            }
            return types;
        }

        private int split(String line) {
            // final StringTokenizer st = new StringTokenizer(line, delim,true);
            final String[] values = line.split(delim);

//            System.out.println(line);
            if (values.length < lineSize) {
                System.err.println("ERROR: line has fewer than expected fields (" + lineSize + ")");
                System.err.println(line);
                System.exit(1); // ABK TODO: manage error codes
            }
            int count=0;
            for (int i = 0; i < lineSize; i++) {
                // String value = st.nextToken();
                String value = values[i];
                lineData[i] = value.trim().isEmpty() ? null : value;
                if (i >= offset && lineData[i]!=null) {
                    data[count++]=fields[i];
                    data[count++]=types[i].convert(lineData[i]);
                }
            }
            return count;
        }

        public Map<String,Object> update(String line, Object... header) {
            int nonNullCount = split(line);
            if (header.length > 0) {
                System.arraycopy(lineData, 0, header, 0, header.length);
            }

            if (nonNullCount == dataSize*2) {
                return map(data);
            }
            Object[] newData=new Object[nonNullCount];
            System.arraycopy(data,0,newData,0,nonNullCount);
            return map(newData);
        }

    }

    static class StdOutReport implements Report {
        private final long batch;
        private final long dots;
        private long count;
        private long total = System.currentTimeMillis(), time, batchTime;

        public StdOutReport(long batch, int dots) {
            this.batch = batch;
            this.dots = batch / dots;
        }

        @Override
        public void reset() {
            count = 0;
            batchTime = time = System.currentTimeMillis();
        }

        @Override
        public void finish() {
            System.out.println("\nTotal import time: "+ (System.currentTimeMillis() - total) / 1000 + " seconds ");
        }

        @Override
        public void dots() {
            if ((++count % dots) != 0) return;
            System.out.print(".");
            if ((count % batch) != 0) return;
            long now = System.currentTimeMillis();
            System.out.println(" "+ (now - batchTime) + " ms for "+batch);
            batchTime = now;
        }

        @Override
        public void finishImport(String type) {
            System.out.println("\nImporting " + count + " " + type + " took " + (System.currentTimeMillis() - time) / 1000 + " seconds ");
        }
    }

    void importIndiv(Reader reader, int flag) throws IOException {
        String[] strTemp;
        BufferedReader bf = new BufferedReader(reader);
        final Data data = new Data(bf.readLine(), "\\|", 0);
        String line;
        report.reset();
        LuceneBatchInserterIndexProviderNewImpl indexProvider = new LuceneBatchInserterIndexProviderNewImpl(db); 	
        	BatchInserterIndex idxIndivContrib = indexProvider.nodeIndex( "individuals", MapUtil.stringMap( "type", "exact" ) );
        	idxIndivContrib.setCacheCapacity( "indivName", 2000000 );
        while ((line = bf.readLine()) != null) {
        	strTemp = line.split("\\|");
        	long caller = db.createNode(data.update(line));
        	//System.out.println(caller);
        	Map<String, Object> properties = MapUtil.map( "indivName", strTemp[1]);
    		properties.put("indivCity", strTemp[2]);
    		properties.put("indivState", strTemp[3]);
    		properties.put("indivZip", strTemp[4]);
    		properties.put("indivOCC", strTemp[6]);
    		idxIndivContrib.add(caller,properties);
        	cache.put(strTemp[0], caller);
           
            report.dots();
        }
        idxIndivContrib.flush();
        indexProvider.shutdown();
        report.finishImport("Nodes");
    }
    
    
    void importNames(String nodeName) throws IOException {
    	Connection con = null;
        Label organizationLabel = DynamicLabel.label("organization");
        //get all the files from a directory
		BatchInserterIndexProvider indexProvider =new LuceneBatchInserterIndexProvider( db );
		BatchInserterIndex organizations = indexProvider.nodeIndex( "organizations", MapUtil.stringMap( "type", "exact" ) );
		organizations.setCacheCapacity( "organizations", 1000000 );
		
		try {

			Class.forName(JDBC_DRIVER_NAME);

			con = DriverManager.getConnection(CONNECTION_URL);

			Statement stmt = con.createStatement();

			ResultSet rs = stmt.executeQuery(SQL_STATEMENT);

			System.out.println("\n== Begin Query Results ======================");
			// print the results to the console
			while (rs.next()) {
				// the example query returns one String column
				//System.out.println(rs.getString(1));

    			Long lOrgId = cache.get(rs.getString(1));
    			if (lOrgId!=null){
    			}else{
    				Map<String, Object> properties = MapUtil.map( "name", rs.getString(1));	 	
    				long node = db.createNode( properties, organizationLabel );
    				organizations.add( node, properties );
    				cache.put(rs.getString(1), node);
    			}
    			report.dots();

			}

		    System.out.println("finished");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}
            
        //make the changes visible for reading, use this sparsely, requires IO!
        organizations.flush();      
   		// Make sure to shut down the index provider as well
		indexProvider.shutdown();

    }    
    


    static class RelType implements RelationshipType {
        String name;

        public RelType update(Object value) {
            this.name = value.toString();
            return this;
        }

        public String name() {
            return name;
        }
    }

    public enum Type {
        BOOLEAN {
            @Override
            public Object convert(String value) {
                return Boolean.valueOf(value);
            }
        },
        INT {
            @Override
            public Object convert(String value) {
                return Integer.valueOf(value);
            }
        },
        LONG {
            @Override
            public Object convert(String value) {
                return Long.valueOf(value);
            }
        },
        DOUBLE {
            @Override
            public Object convert(String value) {
                return Double.valueOf(value);
            }
        },
        FLOAT {
            @Override
            public Object convert(String value) {
                return Float.valueOf(value);
            }
        },
        BYTE {
            @Override
            public Object convert(String value) {
                return Byte.valueOf(value);
            }
        },
        SHORT {
            @Override
            public Object convert(String value) {
                return Short.valueOf(value);
            }
        },
        CHAR {
            @Override
            public Object convert(String value) {
                return value.charAt(0);
            }
        },
        STRING {
            @Override
            public Object convert(String value) {
                return value;
            }
        };

        private static Type fromString(String typeString) {
            if (typeString==null || typeString.isEmpty()) return Type.STRING;
            try {
                return valueOf(typeString.toUpperCase());
            } catch (Exception e) {
                throw new IllegalArgumentException("Unknown Type "+typeString);
            }
        }

        public abstract Object convert(String value);
    }


}