package foo

import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource
import chemaxon.jchem.db.cache.CacheManager

/**
 *
 * @author timbo
 */
class AbstractDataSource {
    
    static DataSource createDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource()
        
        ds.serverName = System.getenv("CHEMCENTRAL_DB_SERVER") ?: 'localhost'
        ds.portNumber =  new Integer(System.getenv("CHEMCENTRAL_DB_PORT") ?: '5432')
        ds.databaseName = System.getenv("CHEMCENTRAL_DB_NAME") ?: 'chemcentral'
        ds.user = System.getenv("CHEMCENTRAL_DB_USERNAME") ?: 'chemcentral'
        ds.password =  System.getenv("CHEMCENTRAL_DB_PASSWORD") ?:  'chemcentral'

        return ds
    }
    
    static String buildMemoryInfo() {
        System.gc()
        StringBuilder b = new StringBuilder("Free memory: ")
        b.append(Runtime.getRuntime().freeMemory())
        .append("\nTotal memory: ")
        .append(Runtime.getRuntime().totalMemory())
        .append("\nMax memory: ")
        .append(Runtime.getRuntime().maxMemory())
        .append("\n")
        
        return b.toString()
    }
    
    static String buildCacheInfo() {
        StringBuilder b = new StringBuilder("Cache details:\n");
        Map<String, Long> tables = CacheManager.INSTANCE.getCachedTables();
        tables.each { k,v -> 
            b.append(k).append(" -> ").append(v).append("\n")
        }
        
        return b.toString()
    }
	
}

