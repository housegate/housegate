# remote() Sniffing Results

## Packet Type Statistics
```
Hello             : 14
Query             : 17
Data              : 7
Ping              : 6
TablesStatusRequest: 9
unknown           : 0
```
- All common packet types are identified, unknown=0, no missing statistics.

## Captured Packet Contents (Partial)
- Query packet examples:
  - `DESC TABLE system.numbers`
  - `SELECT sum(number) FROM system.numbers`
  - `CREATE TABLE default.t_mem (n UInt32) ENGINE=Memory;`
  - `INSERT INTO FUNCTION remote('172.17.0.2:9001', 'default', 't_mem', 'default', '') VALUES ...`
  - `SELECT sum(n) FROM remote('172.17.0.2:9001', 'default', 't_mem', 'default', '')`
  - `SELECT name FROM system.tables WHERE database='default' LIMIT 5`
- Data packet examples (low readability):
  - `Data packet (99 bytes): ... n UInt32 ...`
  - `Data packet (61 bytes): g C +   n UInt32`
  - `Data packet (38 bytes): l \z| F`


## Conclusion
- During remote() function execution, Hello/Query/Data/Ping/TablesStatusRequest packet types appeared, no unknown types were encountered.