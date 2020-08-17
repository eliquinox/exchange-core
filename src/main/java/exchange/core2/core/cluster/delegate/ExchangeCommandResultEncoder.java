package exchange.core2.core.cluster.delegate;

import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

import static exchange.core2.core.cluster.utils.SerializationUtils.serializeL2MarketData;

public class ExchangeCommandResultEncoder {

    static int encodeResultCode(CommandResultCode resultCode, MutableDirectBuffer buffer, int offset) {
        buffer.putInt(offset, resultCode.getCode());
        return offset + BitUtil.SIZE_OF_INT;
    }

    public static void encodeExchangeCommandResponse(OrderCommand cmd, MutableDirectBuffer buffer, int offset) {
        int currentOffset = encodeResultCode(cmd.resultCode, buffer, offset);
        switch (cmd.command) {
            case ORDER_BOOK_REQUEST:
                L2MarketData marketData = cmd.marketData;
                if (marketData != null)
                    serializeL2MarketData(cmd.marketData, buffer, currentOffset);
                break;
            case PLACE_ORDER:
            case REDUCE_ORDER:
            case CANCEL_ORDER:
            case MOVE_ORDER:
            default:
                break;
        }
    }
}
