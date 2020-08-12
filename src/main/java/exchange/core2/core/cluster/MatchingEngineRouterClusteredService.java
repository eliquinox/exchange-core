package exchange.core2.core.cluster;


import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.ExchangeConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import exchange.core2.core.common.config.SerializationConfiguration;
import exchange.core2.core.processors.MatchingEngineRouter;
import exchange.core2.core.processors.SharedPool;
import exchange.core2.core.processors.journaling.ISerializationProcessor;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import lombok.extern.slf4j.Slf4j;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;

import static exchange.core2.core.ExchangeCore.EVENTS_POOLING;
import static exchange.core2.core.cluster.handlers.Decoders.decodeMatchingEngineCommand;

/**
 * Aeron cluster service encapsulating single-sharded {@link exchange.core2.core.processors.MatchingEngineRouter}
 */

@Slf4j
public class MatchingEngineRouterClusteredService implements ClusteredService {

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableDirectByteBuffer(512);
    private IdleStrategy idleStrategy;
    private MatchingEngineRouter matchingEngineRouter;

    public MatchingEngineRouterClusteredService(ExchangeConfiguration exchangeCfg) {
        final PerformanceConfiguration perfCfg = exchangeCfg.getPerformanceCfg();
        final SerializationConfiguration serializationCfg = exchangeCfg.getSerializationCfg();

        final int matchingEnginesNum = perfCfg.getMatchingEnginesNum();
        final int riskEnginesNum = perfCfg.getRiskEnginesNum();
        final int poolInitialSize = (matchingEnginesNum + riskEnginesNum) * 8;
        final int chainLength = EVENTS_POOLING ? 1024 : 1;
        final SharedPool sharedPool = new SharedPool(poolInitialSize * 4, poolInitialSize, chainLength);
        final ISerializationProcessor serializationProcessor = serializationCfg
                .getSerializationProcessorFactory()
                .apply(exchangeCfg);

        this.matchingEngineRouter = new MatchingEngineRouter(
                1,
                1,
                serializationProcessor,
                perfCfg.getOrderBookFactory(),
                sharedPool,
                exchangeCfg
        );
    }

    @Override
    public void onStart(Cluster cluster, Image image) {
        log.info("Cluster service started");
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

        int currentOffset = offset;
        OrderCommandType orderCommandType = OrderCommandType.fromCode(buffer.getByte(currentOffset));
        currentOffset += BitUtil.SIZE_OF_BYTE;

        OrderCommand cmd = decodeMatchingEngineCommand(orderCommandType, buffer, currentOffset, timestamp);

        // TODO: send response
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
