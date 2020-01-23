package info.jerrinot.hazelcastbridge.jetsource;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.EventJournalMapEvent;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.pipeline.Sinks.logger;

public class End2EndTest {
    private static final String CLIENT_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<hazelcast-client xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "                  xsi:schemaLocation=\"http://www.hazelcast.com/schema/client-config\n"
            + "                               http://www.hazelcast.com/schema/client-config/hazelcast-client-config-3.11.xsd\"\n"
            + "                  xmlns=\"http://www.hazelcast.com/schema/client-config\">\n"
            + "    <network>\n"
            + "        <cluster-members>\n"
            + "            <address>127.0.0.1:5701</address>\n"
            + "        </cluster-members>\n"
            + "    </network>\n"
            + "    <group>\n"
            + "        <name>dev</name>\n"
            + "        <password>1234</password>\n"
            + "    </group>\n"
            + "</hazelcast-client>\n";

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        StreamSource<EventJournalMapEvent<Integer, String>> myMap = Hazelcast3Sources.mapJournal("myMap", CLIENT_XML, START_FROM_CURRENT);
        pipeline.readFrom(myMap)
                .withIngestionTimestamps()
                .writeTo(logger());

        JetInstance jetInstance = Jet.newJetInstance();
        jetInstance.newJob(pipeline).join();
    }
}
