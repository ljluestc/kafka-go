package protocol

type RequestPartition struct {
	CommittedLeaderEpoch int32 `kafka:"min=v6"` // Updated from v5 to v6
}
