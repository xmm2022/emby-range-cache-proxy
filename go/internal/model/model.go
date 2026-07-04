package model

type ByteRange struct {
	Start int64
	End   int64
}

func (r ByteRange) Length() int64 {
	return r.End - r.Start + 1
}

type RequestContext struct {
	Method        string
	RawPath       string
	ItemID        string
	MediaSourceID string
	Token         string
	Extension     string
	PlaySessionID string
	DeviceID      string
}

type MediaSource struct {
	ItemID        string
	MediaSourceID string
	Path          string
	Protocol      string
	Size          *int64
	Container     string
	Bitrate       *int64
}

type SourceMetadata struct {
	URL          string
	Size         int64
	ContentType  string
	ETag         string
	LastModified string
}
