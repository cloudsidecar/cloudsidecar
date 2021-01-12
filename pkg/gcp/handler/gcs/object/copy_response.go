package object

type CopyResponse struct {
	Kind string `json:"kind"`
	TotalBytesRewritten int64 `json:"totalBytesRewritten"`
	ObjectSize int64 `json:"objectSize"`
	Done bool `json:"done"`
	Resource string `json:"resource"`
}
