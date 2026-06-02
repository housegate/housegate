package indexingusage

// MapTableTypeToSKU translates a registry.Table.Type value (the
// driver-set classification stored when CREATE TABLE was registered
// on-chain) into the on-chain SKU name sentio-node expects.
//
// Mapping mirrors driver/controller/startup/quota.go in sentioxyz/sentio:
// every metric-shaped table family rolls up to the "metric" SKU,
// event-shaped tables to "event", entity tables to "entity". Webhook
// usage doesn't go through ClickHouse writes and therefore has no
// table-type signal to map.
//
// Returns ("", false) for any tableType that should NOT contribute to
// indexing-usage billing: user-created tables, views, materialised
// views, and any tableType the driver might add later that we have not
// yet been taught about. Forward-compat over silent over-billing.
func MapTableTypeToSKU(tableType string) (string, bool) {
	switch tableType {
	case "counter", "gauge":
		return "metric", true
	case "event":
		return "event", true
	case "entity":
		return "entity", true
	default:
		return "", false
	}
}
