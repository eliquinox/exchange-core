package exchange.core2.core.cluster.delegate;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.binary.BinaryCommandType;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.util.function.Consumer;

/**
 * The following module is responsible for decoding and delegating state change commands coming into cluster
 * from cluster clients. Such commands are encoded into {@link org.agrona.DirectBuffer} as per Aeron Cluster
 * specification and need to be parsed accordingly to be executed by {@link exchange.core2.core.ExchangeApi}.
 */

@Slf4j
@RequiredArgsConstructor
public class ExchangeClusterDelegate {

    private final ExchangeApi exchangeApi;

    public void delegateExchangeCommand(DirectBuffer buffer, int offset, Consumer<OrderCommand> callback) {
        int currentOffset = offset;
        OrderCommandType command = OrderCommandType.fromCode(buffer.getByte(offset));
        currentOffset += BitUtil.SIZE_OF_BYTE;
        switch (command) {
            case ORDER_BOOK_REQUEST:
                decodeOrderBookRequest(buffer, currentOffset, callback);
                break;
            case PLACE_ORDER:
                decodePlaceOrder(buffer, currentOffset, callback);
                break;
            case REDUCE_ORDER:
                decodeReduceOrder(buffer, currentOffset, callback);
                break;
            case CANCEL_ORDER:
                decodeCancelOrder(buffer, currentOffset, callback);
                break;
            case MOVE_ORDER:
                decodeMoveOrder(buffer, currentOffset, callback);
                break;
            case BINARY_DATA_COMMAND:
                // |---int binaryCommandType---|---...---|
                BinaryCommandType binaryCommandType = BinaryCommandType.of(buffer.getInt(currentOffset));
                currentOffset += BitUtil.SIZE_OF_INT;
                switch (binaryCommandType) {
                    case ADD_SYMBOLS:
                        decodeAddSymbols(buffer, currentOffset, callback);
                        break;
                    case ADD_ACCOUNTS:
                        log.info("Unsupported command: {}", binaryCommandType);
                        break;
                }
                break;
            default:
                log.debug("Unsupported command: {}", command);
        }
    }

    private void decodeAddSymbols(DirectBuffer buffer, int offset, Consumer<OrderCommand> callback) {
        // |---int symbolId---|---byte type---|---int baseCurrency---|---int quoteCurrency---|
        // |---long baseScale---|---long quoteScale---|---long takerFee---|---long makerFee---|
        int typeOffset = offset + BitUtil.SIZE_OF_INT;
        int baseCurrencyOffset = typeOffset + BitUtil.SIZE_OF_BYTE;
        int quoteCurrencyOffset = baseCurrencyOffset + BitUtil.SIZE_OF_INT;
        int baseScaleOffset = quoteCurrencyOffset + BitUtil.SIZE_OF_INT;
        int quoteScaleOffset = baseScaleOffset + BitUtil.SIZE_OF_LONG;
        int takerFeeOffset = quoteScaleOffset + BitUtil.SIZE_OF_LONG;
        int makerFeeOffset = takerFeeOffset + BitUtil.SIZE_OF_LONG;

        int symbolId = buffer.getInt(offset);
        SymbolType symbolType = SymbolType.of(buffer.getByte(typeOffset));
        int baseCurrency = buffer.getInt(baseCurrencyOffset);
        int quoteCurrency = buffer.getInt(quoteCurrencyOffset);
        long baseScale = buffer.getLong(baseScaleOffset);
        long quoteScale = buffer.getLong(quoteScaleOffset);
        long takerFee = buffer.getLong(takerFeeOffset);
        long makerFee = buffer.getLong(makerFeeOffset);

        CoreSymbolSpecification symbolSpecXbtLtc = CoreSymbolSpecification.builder()
                .symbolId(symbolId)
                .type(symbolType)
                .baseCurrency(baseCurrency)
                .quoteCurrency(quoteCurrency)
                .baseScaleK(baseScale)
                .quoteScaleK(quoteScale)
                .takerFee(takerFee)
                .makerFee(makerFee)
                .build();

        CommandResultCode commandResultCode = exchangeApi
                .submitBinaryDataAsync(new BatchAddSymbolsCommand(symbolSpecXbtLtc))
                .join();

        callback.accept(OrderCommand.builder()
                .command(OrderCommandType.BINARY_DATA_COMMAND)
                .resultCode(commandResultCode)
                .build());
    }

    private void decodeOrderBookRequest(DirectBuffer buffer, int offset, Consumer<OrderCommand> callback) {
        // |---int symbolId---|---int depth---|
        int depthOffset = offset + BitUtil.SIZE_OF_INT;

        int symbolId = buffer.getInt(offset);
        int depth = buffer.getInt(depthOffset);

        exchangeApi.orderBookRequest(symbolId, depth, callback);
    }

    private void decodePlaceOrder(DirectBuffer buffer, int offset, Consumer<OrderCommand> callback) {
        // |---long uid---|---long price---|---long reservedPrice---|---long size---|
        // |---byte orderAction---|---byte orderType---|---int symbolId---|---int userCookie---|
        int priceOffset = offset + BitUtil.SIZE_OF_LONG;
        int reservedPriceOffset = priceOffset + BitUtil.SIZE_OF_LONG;
        int sizeOffset = reservedPriceOffset + BitUtil.SIZE_OF_LONG;
        int orderActionOffset = sizeOffset + BitUtil.SIZE_OF_LONG;
        int orderTypeOffset = orderActionOffset + BitUtil.SIZE_OF_BYTE;
        int symbolIdOffset = orderTypeOffset + BitUtil.SIZE_OF_BYTE;
        int userCookieOffset = symbolIdOffset + BitUtil.SIZE_OF_INT;

        long uid = buffer.getLong(offset);
        long price = buffer.getLong(priceOffset);
        long reservedPrice = buffer.getLong(reservedPriceOffset);
        long size = buffer.getLong(sizeOffset);
        OrderAction orderAction = OrderAction.of(buffer.getByte(orderActionOffset));
        OrderType orderType = OrderType.of(buffer.getByte(orderTypeOffset));
        int symbolId = buffer.getInt(symbolIdOffset);
        int userCookie = buffer.getInt(userCookieOffset);

        exchangeApi.placeNewOrder(
                userCookie,
                price,
                reservedPrice,
                size,
                orderAction,
                orderType,
                symbolId,
                uid,
                callback
        );
    }

    private void decodeReduceOrder(DirectBuffer buffer, int offset, Consumer<OrderCommand> callback) {
        // |---long uid---|---long orderId---|---long reduceSize---|---int symbolId---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int reduceSizeOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int symbolIdOffset = reduceSizeOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long reduceSize = buffer.getLong(reduceSizeOffset);
        int symbolId = buffer.getInt(symbolIdOffset);

        exchangeApi.reduceOrder(reduceSize, orderId, symbolId, uid, callback);
    }

    private void decodeCancelOrder(DirectBuffer buffer, int offset, Consumer<OrderCommand> callback) {
        // |---long uid---|---long orderId---|---int symbolId---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int symbolIdOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        int symbolId = buffer.getInt(symbolIdOffset);

        exchangeApi.cancelOrder(orderId, symbolId, uid, callback);
    }

    private void decodeMoveOrder(DirectBuffer buffer, int offset, Consumer<OrderCommand> callback) {
        // |---long uid---|---long orderId---|---long newPrice---|---int symbolId---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int newPriceOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int symbolIdOffset = newPriceOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long newPrice = buffer.getLong(newPriceOffset);
        int symbolId = buffer.getInt(symbolIdOffset);

        exchangeApi.moveOrder(newPrice, orderId, symbolId, uid, callback);
    }
}
