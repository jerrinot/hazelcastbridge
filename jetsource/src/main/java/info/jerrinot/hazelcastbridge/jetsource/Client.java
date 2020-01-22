package info.jerrinot.hazelcastbridge.jetsource;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import hz3bridge.Foo;

public class Client {
    public static void main(String[] args) {
        Foo.foo();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5702");
        HazelcastInstance hz4client = HazelcastClient.newHazelcastClient(clientConfig);
    }
}
