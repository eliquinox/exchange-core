package exchange.core2.core.cluster;

import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.api.binary.BinaryCommandType;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.ExchangeConfiguration;
import io.aeron.Image;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class TestExchangeClusteredService {

    private static final ExchangeConfiguration EXCHANGE_CONFIGURATION = ExchangeConfiguration.defaultBuilder().build();
    private ClientSession clientSession;
    private ExchangeClusteredService service;
    private final Cluster clusterMock = mock(Cluster.class);
    private final Image imageMock = mock(Image.class);
    private final Header headerMock = new Header(0, 0);
    private List<OrderCommand> cmdList;

    @Before
    public void init() {
        clientSession = mock(ClientSession.class);
        cmdList = new ArrayList<>();
        service = new ExchangeClusteredService(EXCHANGE_CONFIGURATION, (cmd, l) -> {
            cmdList.add(cmd);
        });
        service.onStart(clusterMock, imageMock);
    }

    @Test
    public void testAddSymbolsCommand() {
        // incoming buffer from cluster client
        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        int offset = 0;

        byte orderCommandType = OrderCommandType.BINARY_DATA_COMMAND.getCode();
        buffer.putByte(offset, orderCommandType);
        offset += BitUtil.SIZE_OF_BYTE;

        int binaryCommandType = BinaryCommandType.ADD_SYMBOLS.getCode();
        buffer.putInt(offset, binaryCommandType);
        offset += BitUtil.SIZE_OF_INT;

        int symbolXbtLtc = 241;
        buffer.putInt(offset, symbolXbtLtc);
        offset += BitUtil.SIZE_OF_INT;

        byte type = SymbolType.CURRENCY_EXCHANGE_PAIR.getCode();
        buffer.putByte(offset, type);
        offset += BitUtil.SIZE_OF_BYTE;

        int baseCurrencyCodeXbt = 11;
        buffer.putInt(offset, baseCurrencyCodeXbt);
        offset += BitUtil.SIZE_OF_INT;

        int quoteCurrencyCodeLtc = 15;
        buffer.putInt(offset, quoteCurrencyCodeLtc);
        offset += BitUtil.SIZE_OF_INT;

        long baseScale = 10000;
        buffer.putLong(offset, baseScale);
        offset += BitUtil.SIZE_OF_LONG;

        long quoteScale = 10000;
        buffer.putLong(offset, quoteScale);
        offset += BitUtil.SIZE_OF_LONG;

        long takerFee = 100L;
        buffer.putLong(offset, takerFee);
        offset += BitUtil.SIZE_OF_LONG;

        long makerFee = 100L;
        buffer.putLong(offset, makerFee);

        //simulate message being sent to cluster
        service.onSessionMessage(clientSession, System.nanoTime(), buffer, 0, 128, headerMock);
        ArgumentCaptor<DirectBuffer> bufferArgumentCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(clientSession, times(1)).offer(
                bufferArgumentCaptor.capture(),
                offsetCaptor.capture(),
                lengthCaptor.capture()
        );

        DirectBuffer egressBuffer = bufferArgumentCaptor.getValue();
        int commandResult = egressBuffer.getInt(0);
        assert commandResult == CommandResultCode.SUCCESS.getCode();
        assert offsetCaptor.getValue() == 0;
        assert lengthCaptor.getValue() == 512;
        assert cmdList.size() == 2;
        OrderCommand cmd1 = cmdList.get(0);
        OrderCommand cmd2 = cmdList.get(1);
        assert cmd1.command == OrderCommandType.BINARY_DATA_COMMAND;
        assert cmd2.command == OrderCommandType.BINARY_DATA_COMMAND;
        assert cmd1.resultCode == CommandResultCode.ACCEPTED;
        assert cmd2.resultCode == CommandResultCode.SUCCESS;
    }
}
