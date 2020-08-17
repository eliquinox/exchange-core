package exchange.core2.core.cluster;


import exchange.core2.core.ExchangeCore;
import exchange.core2.core.cluster.delegate.ExchangeClusterDelegate;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.config.ExchangeConfiguration;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import lombok.extern.slf4j.Slf4j;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;

import java.util.function.ObjLongConsumer;

import static exchange.core2.core.cluster.delegate.ExchangeCommandResultEncoder.encodeExchangeCommandResponse;

/**
 * Aeron cluster service encapsulating {@link exchange.core2.core.ExchangeCore}
 */

@Slf4j
public class ExchangeClusteredService implements ClusteredService {

    private IdleStrategy idleStrategy;
    private final ExchangeCore exchangeCore;
    private final ExchangeClusterDelegate exchangeClusterDelegate;

    public ExchangeClusteredService(ExchangeConfiguration exchangeCfg, ObjLongConsumer<OrderCommand> resultsConsumer) {
        this.exchangeCore = ExchangeCore.builder()
                .exchangeConfiguration(exchangeCfg)
                .resultsConsumer(resultsConsumer)
                .build();

        this.exchangeClusterDelegate = new ExchangeClusterDelegate(exchangeCore.getApi());
    }

    @Override
    public void onStart(Cluster cluster, Image image) {
        log.info("Cluster service started");
        exchangeCore.startup();
        this.idleStrategy = cluster.idleStrategy();
    }

    @Override
    public void onSessionOpen(ClientSession clientSession, long timestamp) {
        log.info("Client session {} opened at {}", clientSession, timestamp);
    }

    @Override
    public void onSessionClose(ClientSession clientSession, long timestamp, CloseReason closeReason) {
        log.info("Client session {} closed at {} because of {}", clientSession, timestamp, closeReason);
    }

    @Override
    public void onSessionMessage(
            ClientSession clientSession,
            long timestamp,
            DirectBuffer buffer,
            int offset,
            int length,
            Header header
    ) {
        log.info("Cluster received message {} from session {} at {}", buffer, clientSession, timestamp);
        exchangeClusterDelegate.delegateExchangeCommand(buffer, offset, (cmd) -> {
            // check that a node is not in replay mode
            if (null != clientSession) {
                // Respond to cluster client
                MutableDirectBuffer egressMessageBuffer = new ExpandableDirectByteBuffer(512);
                encodeExchangeCommandResponse(cmd, egressMessageBuffer, 0);
                log.info("Responding with {}", egressMessageBuffer);
                while (clientSession.offer(egressMessageBuffer, 0, 512) < 0) {
                    idleStrategy.idle();
                }
            }
        });
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        log.info("Timer event of correlation ID {} at {}", correlationId, timestamp);
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication exclusivePublication) {
        log.info("Take snapshot event");
    }

    @Override
    public void onRoleChange(Cluster.Role role) {
        log.info("Role changed to {}", role);
    }

    @Override
    public void onTerminate(Cluster cluster) {
        log.info("Cluster terminated");
    }
}
