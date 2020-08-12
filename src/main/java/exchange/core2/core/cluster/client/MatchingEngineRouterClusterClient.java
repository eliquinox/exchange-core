package exchange.core2.core.cluster.client;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.stream.IntStream;

import static exchange.core2.core.cluster.utils.NetworkUtils.ingressEndpoints;
import static java.util.stream.Collectors.toList;

@Slf4j
public class MatchingEngineRouterClusterClient implements EgressListener {

    private AeronCluster aeronCluster;
    private final AeronCluster.Context clusterContext;
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();

    public MatchingEngineRouterClusterClient(
            //TODO: make client configurable via ExchangeConfiguration
            String aeronDirName,
            String ingressHost,
            String egressHost,
            int egressPort,
            boolean deleteOnStart
    ) {
        MediaDriver clientMediaDriver = MediaDriver.launchEmbedded(
                new MediaDriver.Context()
                        .threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(deleteOnStart)
                        .errorHandler(Throwable::printStackTrace)
                        .aeronDirectoryName(aeronDirName)
        );

        String egressChannelEndpoint = egressHost + ":" + egressPort;
        this.clusterContext =  new AeronCluster.Context()
                .egressListener(this)
                .egressChannel("aeron:udp?endpoint=" + egressChannelEndpoint)
                .aeronDirectoryName(clientMediaDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp")
                // TODO: make number of nodes in cluster and their hosts configurable
                .ingressEndpoints(ingressEndpoints(IntStream.range(0, 3).mapToObj(i -> ingressHost).collect(toList())));
    }

    public void connectToCluster() {
        this.aeronCluster = AeronCluster.connect(clusterContext);
    }

    @Override
    public void onMessage(
            long clusterSessionId,
            long timestamp,
            DirectBuffer directBuffer,
            int offset,
            int length,
            Header header
    ) {


    }
}
