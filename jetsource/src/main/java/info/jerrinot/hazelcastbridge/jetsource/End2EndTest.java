package info.jerrinot.hazelcastbridge.jetsource;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.pipeline.Sinks.logger;

public class End2EndTest {
    private static final String CLIENT_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<!--\n"
            + "  ~ Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.\n"
            + "  ~\n"
            + "  ~ Licensed under the Apache License, Version 2.0 (the \"License\");\n"
            + "  ~ you may not use this file except in compliance with the License.\n"
            + "  ~ You may obtain a copy of the License at\n"
            + "  ~\n"
            + "  ~ http://www.apache.org/licenses/LICENSE-2.0\n"
            + "  ~\n"
            + "  ~ Unless required by applicable law or agreed to in writing, software\n"
            + "  ~ distributed under the License is distributed on an \"AS IS\" BASIS,\n"
            + "  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
            + "  ~ See the License for the specific language governing permissions and\n"
            + "  ~ limitations under the License.\n"
            + "  -->\n"
            + "\n"
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

        StreamSource<Object> myMap = Sources.iMap3Journal("myMap", CLIENT_XML, START_FROM_CURRENT, null, null);
        pipeline.readFrom(myMap)
                .withIngestionTimestamps()
                .writeTo(logger());

        JetInstance jetInstance = Jet.newJetInstance();
        jetInstance.newJob(pipeline).join();
    }
}
