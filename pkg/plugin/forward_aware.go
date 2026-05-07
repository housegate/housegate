package plugin

// ForwardAware lets a plugin opt out of running on a session that the
// forward-decision plugin has marked as a transparent forward to a peer's
// internal-port. Default (no implementation, or RunOnForward()==true) =
// the plugin runs.
//
// Mirror of PeerTrustAware. Used for plugins like rewrite / commitgate
// whose work belongs on the host proxy, not the entry proxy.
type ForwardAware interface {
	RunOnForward() bool
}
