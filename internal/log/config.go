package log

type Config struct {
	Segment Segment
}

type Segment struct {
	MaxStoreBytes uint64
	MaxIndexBytes uint64
	InitialOffSet uint64
}
