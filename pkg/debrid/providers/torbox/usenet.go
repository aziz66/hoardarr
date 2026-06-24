package torbox

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// TorBox's usenet API mirrors its torrent API: createusenetdownload (NZB upload)
// → mylist?id= (same fields) → requestdl?usenet_id=&file_id=. Usenet files use a
// "torbox-usenet://<usenetId>/<fileId>" link scheme so fetchDownloadLink routes
// them to /api/usenet/requestdl instead of /api/torrents/requestdl.

const usenetLinkScheme = "torbox-usenet://"

type usenetCreateResponse APIResponse[struct {
	Hash             string `json:"hash"`
	UsenetDownloadID int    `json:"usenetdownload_id"`
	AuthID           string `json:"auth_id"`
}]

type usenetInfo struct {
	Id               int       `json:"id"`
	Name             string    `json:"name"`
	Hash             string    `json:"hash"`
	Size             int64     `json:"size"`
	DownloadState    string    `json:"download_state"`
	DownloadSpeed    int64     `json:"download_speed"`
	Progress         float64   `json:"progress"`
	Eta              int       `json:"eta"`
	DownloadFinished bool      `json:"download_finished"`
	DownloadPresent  bool      `json:"download_present"`
	CreatedAt        time.Time `json:"created_at"`
	Files            []struct {
		Id           int    `json:"id"`
		Name         string `json:"name"`
		Size         int64  `json:"size"`
		Hash         string `json:"hash"`
		Mimetype     string `json:"mimetype"`
		ShortName    string `json:"short_name"`
		AbsolutePath string `json:"absolute_path"`
	} `json:"files"`
}

type usenetInfoResponse APIResponse[usenetInfo]

// doPostMultipart uploads a single file field (the NZB) to a TorBox endpoint.
func (tb *Torbox) doPostMultipart(endpoint, fieldName, filename string, content []byte, result interface{}) (*http.Response, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	fw, err := w.CreateFormFile(fieldName, filename)
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write(content); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, tb.Host+endpoint, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(resp.Body).Decode(result); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

// SubmitNZB uploads an NZB to TorBox usenet and returns the usenet download id.
func (tb *Torbox) SubmitNZB(name string, content []byte) (string, error) {
	if len(content) == 0 {
		return "", fmt.Errorf("torbox usenet: empty NZB content")
	}
	filename := name
	if !strings.HasSuffix(strings.ToLower(filename), ".nzb") {
		filename += ".nzb"
	}

	var data usenetCreateResponse
	resp, err := tb.doPostMultipart("/api/usenet/createusenetdownload", "file", filename, content, &data)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || data.Data == nil {
		return "", fmt.Errorf("torbox usenet API error: status %d", resp.StatusCode)
	}
	if data.Data.UsenetDownloadID == 0 {
		return "", fmt.Errorf("torbox usenet: empty usenetdownload_id")
	}
	return strconv.Itoa(data.Data.UsenetDownloadID), nil
}

// GetUsenetTorrent returns the usenet download as a types.Torrent so it flows
// through the same entry/mount machinery as debrid torrents. Files carry the
// "torbox-usenet://" link scheme once the download has finished.
func (tb *Torbox) GetUsenetTorrent(usenetID string) (*types.Torrent, error) {
	var res usenetInfoResponse
	resp, err := tb.doGet("/api/usenet/mylist/", map[string]string{"id": usenetID}, &res)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox usenet API error: status %d", resp.StatusCode)
	}
	data := res.Data
	if data == nil {
		return nil, fmt.Errorf("torbox usenet: download %s not found", usenetID)
	}

	t := &types.Torrent{
		Id:               strconv.Itoa(data.Id),
		Name:             data.Name,
		InfoHash:         data.Hash,
		Bytes:            data.Size,
		Size:             data.Size,
		Progress:         data.Progress * 100,
		Status:           tb.getTorboxStatus(data.DownloadState, data.DownloadFinished),
		Speed:            data.DownloadSpeed,
		Filename:         data.Name,
		OriginalFilename: data.Name,
		Debrid:           tb.config.Name,
		Files:            make(map[string]types.File),
		Added:            data.CreatedAt,
	}

	cfg := config.Get()
	for _, f := range data.Files {
		fileName := filepath.Base(f.Name)
		if err := cfg.IsFileAllowed(f.Name, f.Size); err != nil {
			continue
		}
		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Name:      fileName,
			Size:      f.Size,
			Path:      f.Name,
		}
		if data.DownloadFinished {
			file.Link = fmt.Sprintf("%s%s/%d", usenetLinkScheme, t.Id, f.Id)
		}
		t.Files[fileName] = file
	}

	if len(data.Files) > 0 {
		t.OriginalFilename = strings.Split(path.Clean(data.Files[0].Name), "/")[0]
	}
	return t, nil
}

// DeleteUsenetDownload removes a usenet download from TorBox.
func (tb *Torbox) DeleteUsenetDownload(usenetID string) error {
	id, err := strconv.Atoi(usenetID)
	if err != nil {
		return fmt.Errorf("torbox usenet: invalid id %q", usenetID)
	}
	payload := map[string]any{"usenet_id": id, "operation": "delete"}
	resp, err := tb.doDelete("/api/usenet/controlusenetdownload", payload)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox usenet API error: status %d", resp.StatusCode)
	}
	return nil
}

// isUsenetLink reports whether a file link is a TorBox usenet reference.
func isUsenetLink(link string) bool {
	return strings.HasPrefix(link, usenetLinkScheme)
}
