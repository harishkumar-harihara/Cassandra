import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class Fields{
    private String column_name;
    private String data_type;

    public Fields(String column_name, String data_type) {
        this.column_name = column_name;
        this.data_type = data_type;
    }

    public String getColumn_name() {
        return column_name;
    }

    public String getData_type() {
        return data_type;
    }
}

public class CassandraConnector {
    private Cluster cluster;
    private Session session;

    //node=127.0.0.1  //port=9042   based on cqlsh
    public void connect(String node, Integer port){
        Cluster.Builder b = new Cluster.Builder().addContactPoint(node);
        if(port!=null){
            b.withPort(port);
        }
        cluster = b.build();
        session = cluster.connect();
    }

    public Session getSession(){
        return this.session;
    }

    public void close(){
        cluster.close();
        session.close();
    }

    public void create_keyspace(String keyspace_name, String replication_strategy_class, int replication_factor, boolean durable_writes) {

        StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ").append(keyspace_name).
                                    append(" WITH replication = {'class':'").
                                    append(replication_strategy_class).
                                    append("',").append("'replication_factor':").
                                    append(replication_factor).append("}").
                                    append(" AND durable_writes=").
                                    append(durable_writes).append(";");

        String query = sb.toString();
        session.execute(query);
    }

    public void drop_keyspace(String keyspace_name){
        StringBuilder sb = new StringBuilder("DROP KEYSPACE ").append(keyspace_name).append(";");
        String query = sb.toString();
        session.execute(query);
    }

    public void create_table(String keyspace_name,
                             String table_name,
                             List<Fields> fieldsList,
                             List<String> primary_key,
                             List<String> clustering_column)
    {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").
                                        append(keyspace_name).append(".").
                                        append(table_name).
                                        append(" ( ");
        // COLUMNS
        sb.append(fieldsList.stream().map((fields) ->
        {
            return fields.getColumn_name() + " " + fields.getData_type();
        }).collect(Collectors.joining(", ")));

        // PARTITION KEYS
        sb.append(", PRIMARY KEY (( ").
                append(primary_key.stream().map(Objects::toString).collect(Collectors.joining(","))).
                append(")");

        // CLUSTERING COLUMNS
        if(clustering_column.size() > 0){
            sb.append(",").append(clustering_column.stream().map(Objects::toString).
                    collect(Collectors.joining(",")));
        }
        sb.append("));");
        String query = sb.toString();
        session.execute(query);
    }

    public void drop_table(String keyspace_name, String table_name){
        StringBuilder sb = new StringBuilder("DROP TABLE ").append(keyspace_name).append(".").append(table_name).append(";");
        String query = sb.toString();
        session.execute(query);
    }

}
