import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.List;

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

    public static void main(String[] args) {
        CassandraConnector cassandraConnector = new CassandraConnector();
        cassandraConnector.connect("127.0.0.1",9042);
        cassandraConnector.create_keyspace("test_keyspace","SimpleStrategy",1, true);
//        cassandraConnector.drop_keyspace("test_keyspace");
    }
}
