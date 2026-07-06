package storageintegrity

import "testing"

func TestArbiterInterfacesPreserveLegacySequencerCompatibility(t *testing.T) {
	var _ ArbiterIngress = SequencerIngress(nil)
	var _ SequencerIngress = ArbiterIngress(nil)
	var _ ArbiterWorkerClient = SequencerWorkerClient(nil)
	var _ SequencerWorkerClient = ArbiterWorkerClient(nil)
}
