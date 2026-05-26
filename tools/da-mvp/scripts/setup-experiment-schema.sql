CREATE DATABASE IF NOT EXISTS exp;
DROP TABLE IF EXISTS exp.events;
CREATE TABLE exp.events
(
    `id`         UInt64,
    `txn_hash`   FixedString(32),
    `block_num`  UInt64,
    `log_index`  UInt32,
    `contract`   FixedString(20),
    `event_name` String,
    `topic1`     FixedString(32),
    `topic2`     FixedString(32),
    `value`      Decimal128(18),
    `amount`     UInt64,
    `inject_ts`  DateTime64(6),
    `payload`    String
)
ENGINE = MergeTree
ORDER BY (block_num, log_index);
