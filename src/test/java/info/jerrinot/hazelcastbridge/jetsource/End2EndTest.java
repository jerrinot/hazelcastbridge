package info.jerrinot.hazelcastbridge.jetsource;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.map.EventJournalMapEvent;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.logger;

public class End2EndTest {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        StreamSource<EventJournalMapEvent<Integer, String>> myMap = Hazelcast3Sources.mapJournal("myMap", "classpath:my-client-config.xml", START_FROM_OLDEST);
        pipeline.readFrom(myMap)
                .withIngestionTimestamps()
                .writeTo(logger());

        JetInstance jetInstance = Jet.newJetInstance();
        jetInstance.newJob(pipeline).join();
    }
}
