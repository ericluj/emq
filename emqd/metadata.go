package emqd

import "path"

type Metadata struct {
	Topics []TopicMetadata `json:"topics"`
}

type TopicMetadata struct {
	Name     string            `json:"name"`
	Channels []ChannelMetadata `json:"channels"`
}

type ChannelMetadata struct {
	Name string `json:"name"`
}

func metadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "emqd.metadata")
}
