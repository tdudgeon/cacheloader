package foo

import java.sql.ResultSet;
import java.text.MessageFormat;
import javax.sql.DataSource

import chemaxon.util.ConnectionHandler;
import chemaxon.util.SmilesCompressor;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Statement;

/**
 *
 * @author timbo
 */
class CompressionStat extends AbstractDataSource {
    
    public static void main(String[] args) {
    
        DataSource ds = createDataSource()
        Connection con = ds.connection
        con.autoCommit = false
        String propTable = System.getenv("JCHEM_PROPERTY_TABLE") ?: 'chemcentral.jchemproperties'
        String strucTable = System.getenv("JCHEM_STRUCTURE_TABLE") ?: 'chemcentral.structures'
    
        ConnectionHandler connectionHandler = new ConnectionHandler(con, propTable);

        FileWriter writer = new FileWriter("compressionstats-structures.csv");


        Statement stmt = con.createStatement();
        stmt.setFetchSize(10000);
        ResultSet resultSet = stmt.executeQuery("select cd_id, cd_smiles from " + strucTable);

        SmilesCompressor smilesCompressor = new SmilesCompressor();
        /* Some warmup ..*/
        for (int i = 0; i < 100000; i++) {
            byte[] compress = smilesCompressor.compress("CCCCNOC1CCCCCC1CCOOO(COOO)OCO");
        }
        writer.append("CD_ID, UNCOMPRESSED LENGTH, COMPRESSED LENGTH, COMPRESSION TIME(ns)\n");
        int count = 0;
        while (resultSet.next()) {
            count++;
            if (count % 10000 == 0) {
                System.out.println("Processed " + count);
            }
            int cd_id = resultSet.getInt("cd_id");
            String smiles = resultSet.getString("cd_smiles");
            if (smiles != null) {
                long start = System.nanoTime();
                byte[] compressed = smilesCompressor.compress(smiles);
                long end = System.nanoTime();
                long duration = end - start
                String line = MessageFormat.format("{0}, {1}, {2}, {3}\n", cd_id, smiles.getBytes().length,
                    compressed.length, duration);
                writer.append(line);
                
                if (duration > 100000) {
                    println "$cd_id $duration $smiles"
                }
            }
        }
        writer.close();
    
    }
}

