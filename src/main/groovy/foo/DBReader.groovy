package foo

import javax.sql.DataSource
import java.sql.Connection
import groovy.sql.Sql
import org.postgresql.ds.PGSimpleDataSource

/**
 *
 * @author timbo
 */
class DBReader extends AbstractDataSource {
    
    public static void main(String[] args) {
        
        String strucTable = System.getenv("JCHEM_STRUCTURE_TABLE") ?: 'chemcentral.structures'
        
        DataSource ds = createDataSource()
        
        Connection con = ds.connection
        con.setAutoCommit(false)
        Sql db = new Sql(con)
        int count = 0
        println "starting ..."
        def cols = ['cd_id', 'cd_smiles', 'cd_formula', 'cd_sortable_formula', 'cd_molweight', 'cd_hash',
'cd_flags', 'cd_timestamp', 'cd_pre_calculated', 'cd_taut_hash', 'cd_taut_frag_hash', 'cd_screen_descriptor',
'cd_fp1', 'cd_fp2', 'cd_fp3', 'cd_fp4', 'cd_fp5', 'cd_fp6', 'cd_fp7', 'cd_fp8', 'cd_fp9', 'cd_fp10',
'cd_fp11', 'cd_fp12', 'cd_fp13', 'cd_fp14', 'cd_fp15', 'cd_fp16']

        def rows = []
        long t0 = System.currentTimeMillis()
        db.withStatement{ stmt -> stmt.setFetchSize(10000) }
        db.eachRow('SELECT ' + cols.join(',') + ' FROM ' + strucTable) { row ->
            def r = []
            cols.each { c ->
                r << row[c]
            }
            rows << r
            count++
            if ((count % 100000) == 0) {
                println count
            }
        }
        long t1 = System.currentTimeMillis()
        println "Finished in ${t1-t0}ms"

    }
	
}

