package exchange.core2.core.cluster.handlers;

import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

public class Decoders {
    public static OrderCommand decodeMatchingEngineCommand(
            OrderCommandType command,
            DirectBuffer buffer,
            int offset,
            long timestamp
    ) {
        switch (command) {
            case ORDER_BOOK_REQUEST:
                return decodeOrderBookRequest(buffer, offset, timestamp);
            case PLACE_ORDER:
                return decodePlaceOrder(buffer, offset, timestamp);
            case REDUCE_ORDER:
                return decodeReduceOrder(buffer, offset, timestamp);
            case CANCEL_ORDER:
                return decodeCancelOrder(buffer, offset, timestamp);
            case MOVE_ORDER:
                return decodeMoveOrder(buffer, offset, timestamp);
            default:
                return null;
        }
    }

    private static OrderCommand decodeOrderBookRequest(DirectBuffer buffer, int offset, long timestamp) {
        // |---int symbolId---|---int depth---|
        OrderCommand cmd = new OrderCommand();

        int depthOffset = offset + BitUtil.SIZE_OF_INT;
        int symbolId = buffer.getInt(offset);
        int depth = buffer.getInt(depthOffset);

        cmd.command = OrderCommandType.ORDER_BOOK_REQUEST;
        cmd.orderId = -1;
        cmd.symbol = symbolId;
        cmd.uid = -1;
        cmd.depth = depth;
        cmd.timestamp = timestamp;
        cmd.resultCode = CommandResultCode.NEW;

        return cmd;
    }

    private static OrderCommand decodePlaceOrder(DirectBuffer buffer, int offset, long timestamp) {
        // |---long uid---|---long orderId---|---long price---|---long reservedPrice---|---long size---|
        // |---byte orderAction---|---byte orderType---|---int symbolId---|---int userCookie---|
        OrderCommand cmd = new OrderCommand();

        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int priceOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int reservedPriceOffset = priceOffset + BitUtil.SIZE_OF_LONG;
        int sizeOffset = reservedPriceOffset + BitUtil.SIZE_OF_LONG;
        int orderActionOffset = sizeOffset + BitUtil.SIZE_OF_LONG;
        int orderTypeOffset = orderActionOffset + BitUtil.SIZE_OF_BYTE;
        int symbolIdOffset = orderTypeOffset + BitUtil.SIZE_OF_BYTE;
        int userCookieOffset = symbolIdOffset + BitUtil.SIZE_OF_INT;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long price = buffer.getLong(priceOffset);
        long reservedPrice = buffer.getLong(reservedPriceOffset);
        long size = buffer.getLong(sizeOffset);
        OrderAction orderAction = OrderAction.of(buffer.getByte(orderActionOffset));
        OrderType orderType = OrderType.of(buffer.getByte(orderTypeOffset));
        int symbolId = buffer.getInt(symbolIdOffset);
        int userCookie = buffer.getInt(userCookieOffset);

        cmd.command = OrderCommandType.PLACE_ORDER;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        cmd.uid = uid;
        cmd.orderId = orderId; // passed from a cluster client
        cmd.price = price;
        cmd.reserveBidPrice = reservedPrice;
        cmd.size = size;
        cmd.action = orderAction;
        cmd.orderType = orderType;
        cmd.timestamp = timestamp;
        cmd.symbol = symbolId;
        cmd.userCookie = userCookie;

        return cmd;
    }

    private static OrderCommand decodeReduceOrder(DirectBuffer buffer, int offset, long timestamp) {
        // |---long uid---|---long orderId---|---long reduceSize---|---int symbolId---|
        OrderCommand cmd = new OrderCommand();

        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int reduceSizeOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int symbolIdOffset = reduceSizeOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long reduceSize = buffer.getLong(reduceSizeOffset);
        int symbolId = buffer.getInt(symbolIdOffset);

        cmd.command = OrderCommandType.REDUCE_ORDER;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        cmd.uid = uid;
        cmd.orderId = orderId;
        cmd.size = reduceSize;
        cmd.symbol = symbolId;
        cmd.timestamp = timestamp;

        return cmd;
    }

    private static OrderCommand decodeCancelOrder(DirectBuffer buffer, int offset, long timestamp) {
        // |---long uid---|---long orderId---|---int symbolId---|
        OrderCommand cmd = new OrderCommand();

        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int symbolIdOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        int symbolId = buffer.getInt(symbolIdOffset);

        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        cmd.uid = uid;
        cmd.orderId = orderId;
        cmd.symbol = symbolId;
        cmd.timestamp = timestamp;

        return cmd;
    }

    private static OrderCommand decodeMoveOrder(DirectBuffer buffer, int offset, long timestamp) {
        // |---long uid---|---long orderId---|---long newPrice---|---int symbolId---|
        OrderCommand cmd = new OrderCommand();

        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int newPriceOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int symbolIdOffset = newPriceOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long newPrice = buffer.getLong(newPriceOffset);
        int symbolId = buffer.getInt(symbolIdOffset);

        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.resultCode = CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        cmd.uid = uid;
        cmd.orderId = orderId;
        cmd.symbol = symbolId;
        cmd.timestamp = timestamp;

        return cmd;
    }
}
