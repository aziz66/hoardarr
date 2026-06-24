package premiumize

// Premiumize.me API response shapes.
// Every endpoint returns a top-level "status" of "success" or "error" (with a
// "message" on error), embedded here via baseResponse.

type baseResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func (b baseResponse) ok() bool { return b.Status == "success" }

// POST /transfer/create
type createResponse struct {
	baseResponse
	Id   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// A single entry in GET /transfer/list.
// status ∈ {waiting, queued, running, finished, seeding, deleted, banned, error, timeout}
type transfer struct {
	Id       string  `json:"id"`
	Name     string  `json:"name"`
	Status   string  `json:"status"`
	Message  string  `json:"message"`
	Progress float64 `json:"progress"` // 0..1
	FolderId string  `json:"folder_id"`
	FileId   string  `json:"file_id"`
	Src      string  `json:"src"`
}

// GET /transfer/list
type transferListResponse struct {
	baseResponse
	Transfers []transfer `json:"transfers"`
}

// POST/GET /cache/check
type cacheCheckResponse struct {
	baseResponse
	Response []bool   `json:"response"`
	Filename []string `json:"filename"`
	Filesize []int64  `json:"filesize"`
}

// An item inside GET /folder/list content (type ∈ {file, folder}).
type folderItem struct {
	Id         string `json:"id"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	Size       int64  `json:"size"`
	Link       string `json:"link"`
	StreamLink string `json:"stream_link"`
	CreatedAt  int64  `json:"created_at"`
	MimeType   string `json:"mime_type"`
}

// GET /folder/list
type folderListResponse struct {
	baseResponse
	Name     string       `json:"name"`
	FolderId string       `json:"folder_id"`
	ParentId string       `json:"parent_id"`
	Content  []folderItem `json:"content"`
}

// GET /item/details
type itemDetailsResponse struct {
	baseResponse
	Id         string `json:"id"`
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	Link       string `json:"link"`
	StreamLink string `json:"stream_link"`
	FolderId   string `json:"folder_id"`
	MimeType   string `json:"mime_type"`
}

// GET /account/info
type accountInfoResponse struct {
	baseResponse
	CustomerId   any     `json:"customer_id"`
	PremiumUntil int64   `json:"premium_until"`
	LimitUsed    float64 `json:"limit_used"`
	SpaceUsed    float64 `json:"space_used"`
}
