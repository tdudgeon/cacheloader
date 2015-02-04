package foo

import chemaxon.jchem.db.CacheRegistrationUtil
import chemaxon.jchem.db.JChemSearch
import chemaxon.util.ConnectionHandler
import chemaxon.sss.search.JChemSearchOptions
import chemaxon.util.ConnectionHandler
import javax.sql.DataSource
import org.postgresql.ds.PGSimpleDataSource

/**
 *
 * @author timbo
 */
class CacheLoader extends AbstractDataSource {
    
    public static void main(String[] args) {
        DataSource ds = createDataSource()
        def con = ds.connection
        String propTable = System.getenv("JCHEM_PROPERTY_TABLE") ?: 'chemcentral.jchemproperties'
        String strucTable = System.getenv("JCHEM_STRUCTURE_TABLE") ?: 'chemcentral.structures'
            
        ConnectionHandler conh = new ConnectionHandler(con, propTable)

        CacheRegistrationUtil cru = new CacheRegistrationUtil(conh)
        cru.registerCache()
        println "cache registered"
            
        try {
            println "setting autocommit to false"
            con.autoCommit = false
                    
            JChemSearch searcher = new JChemSearch()
            searcher.connectionHandler = conh
            searcher.structureTable = strucTable
            searcher.queryStructure = 'CN1C=NC2=C1C(=O)N(C(=O)N2C)C'
 
            JChemSearchOptions searchOptions = new JChemSearchOptions(JChemSearch.FULL)
            searcher.setSearchOptions(searchOptions)
            searcher.setRunMode(JChemSearch.RUN_MODE_SYNCH_COMPLETE)
            println "starting search"
            long t0 = System.currentTimeMillis()
            searcher.run()
            long t1 = System.currentTimeMillis()
            println "search complete"
            println "autocommit is " + con.autoCommit
            con.commit()
            println "finished in ${t1-t0}ms\n"
            
            println buildCacheInfo() + "\n"
            println buildMemoryInfo() + "\n"

        } finally {
            cru.unRegisterCache()
            println "cache unregistered"
        }
    }
	
}

