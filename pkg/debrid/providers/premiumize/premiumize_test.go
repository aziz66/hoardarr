package premiumize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"go.uber.org/ratelimit"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "decypharr-premiumize-test-*")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// newTestClient builds a Premiumize client whose API base points at a mock server.
func newTestClient(t *testing.T, baseURL string) *Premiumize {
	t.Helper()
	dc := config.Debrid{
		Provider:        "premiumize",
		Name:            "premiumize",
		APIKey:          "test-key",
		DownloadAPIKeys: []string{"test-key"},
		RateLimit:       "1000/second",
	}
	rls := map[string]ratelimit.Limiter{
		"main":     utils.ParseRateLimit(dc.RateLimit),
		"repair":   utils.ParseRateLimit(dc.RateLimit),
		"download": utils.ParseRateLimit(dc.RateLimit),
	}
	p, err := New(dc, rls)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	p.Host = baseURL // redirect all calls to the mock
	return p
}

func TestPremiumizeStatus(t *testing.T) {
	cases := map[string]types.TorrentStatus{
		"finished": types.TorrentStatusDownloaded,
		"seeding":  types.TorrentStatusDownloaded,
		"waiting":  types.TorrentStatusDownloading,
		"queued":   types.TorrentStatusDownloading,
		"running":  types.TorrentStatusDownloading,
		"error":    types.TorrentStatusError,
		"banned":   types.TorrentStatusError,
		"timeout":  types.TorrentStatusError,
		"deleted":  types.TorrentStatusError,
	}
	for in, want := range cases {
		if got := premiumizeStatus(in); got != want {
			t.Errorf("premiumizeStatus(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIsAvailable(t *testing.T) {
	const hashHit = "ABCDEF1234567890ABCDEF1234567890ABCDEF12"
	const hashMiss = "0000000000000000000000000000000000000000"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/cache/check" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		items := r.URL.Query()["items[]"]
		if len(items) != 2 {
			t.Errorf("expected 2 items, got %d", len(items))
		}
		// First item cached, second not.
		w.Write([]byte(`{"status":"success","response":[true,false],"filename":["a.flac",""],"filesize":[123,0]}`))
	}))
	defer srv.Close()

	p := newTestClient(t, srv.URL)
	got := p.IsAvailable([]string{hashHit, hashMiss})

	if !got[hashHit] {
		t.Errorf("expected %s available", hashHit)
	}
	if got[hashMiss] {
		t.Errorf("expected %s NOT available", hashMiss)
	}
}

func TestSubmitMagnet_NotCached(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cache/check":
			w.Write([]byte(`{"status":"success","response":[false]}`))
		case "/transfer/create":
			t.Error("transfer/create must not be called when not cached and DownloadUncached=false")
			w.Write([]byte(`{"status":"success","id":"x"}`))
		}
	}))
	defer srv.Close()

	p := newTestClient(t, srv.URL)
	tr := &types.Torrent{Magnet: &utils.Magnet{
		InfoHash: "ABCDEF1234567890ABCDEF1234567890ABCDEF12",
		Link:     "magnet:?xt=urn:btih:ABCDEF1234567890ABCDEF1234567890ABCDEF12",
		Name:     "Some Album",
	}}
	if _, err := p.SubmitMagnet(tr); err == nil {
		t.Fatal("expected error for uncached magnet, got nil")
	}
}

func TestSubmitMagnet_Cached(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cache/check":
			w.Write([]byte(`{"status":"success","response":[true]}`))
		case "/transfer/create":
			w.Write([]byte(`{"status":"success","id":"tr_42","name":"Some Album","type":"torrent"}`))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	defer srv.Close()

	p := newTestClient(t, srv.URL)
	tr := &types.Torrent{Magnet: &utils.Magnet{
		InfoHash: "ABCDEF1234567890ABCDEF1234567890ABCDEF12",
		Link:     "magnet:?xt=urn:btih:ABCDEF1234567890ABCDEF1234567890ABCDEF12",
		Name:     "Some Album",
	}}
	out, err := p.SubmitMagnet(tr)
	if err != nil {
		t.Fatalf("SubmitMagnet error: %v", err)
	}
	if out.Id != "tr_42" {
		t.Errorf("expected id tr_42, got %q", out.Id)
	}
	if out.Debrid != "premiumize" {
		t.Errorf("expected debrid premiumize, got %q", out.Debrid)
	}
}

func TestUpdateTorrent_FinishedFolder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/transfer/list":
			w.Write([]byte(`{"status":"success","transfers":[
				{"id":"tr_42","name":"Some Album","status":"finished","progress":1,"folder_id":"fld_1"}
			]}`))
		case "/folder/list":
			if got := r.URL.Query().Get("id"); got != "fld_1" {
				t.Errorf("folder/list called with id=%q, want fld_1", got)
			}
			w.Write([]byte(`{"status":"success","name":"Some Album","content":[
				{"id":"f1","name":"01 - Track One.flac","type":"file","size":1000,"link":"` + "http://cdn.example/01.flac" + `"},
				{"id":"f2","name":"cover.jpg","type":"file","size":50,"link":"http://cdn.example/cover.jpg"}
			]}`))
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
		}
	}))
	defer srv.Close()

	p := newTestClient(t, srv.URL)
	tr := &types.Torrent{Id: "tr_42", Files: map[string]types.File{}}
	if err := p.UpdateTorrent(tr); err != nil {
		t.Fatalf("UpdateTorrent error: %v", err)
	}
	if tr.Status != types.TorrentStatusDownloaded {
		t.Errorf("status = %q, want downloaded", tr.Status)
	}
	// .jpg is not in the default allowed music/video extensions → filtered out.
	if _, ok := tr.Files["01 - Track One.flac"]; !ok {
		t.Errorf("expected flac file present, files=%v", keys(tr.Files))
	}
	f := tr.Files["01 - Track One.flac"]
	if f.Id != "f1" || f.Link != "http://cdn.example/01.flac" {
		t.Errorf("unexpected file mapping: %+v", f)
	}
}

func TestFetchDownloadLink_DirectLink(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Must NOT need item/details when the file already has a direct link.
		t.Errorf("no API call expected, got %q", r.URL.Path)
	}))
	defer srv.Close()

	p := newTestClient(t, srv.URL)
	acc := p.accountsManager.Current()
	if acc == nil {
		t.Fatal("no current account")
	}
	file := &types.File{Id: "f1", Name: "01.flac", Size: 1000, Link: "http://cdn.example/01.flac"}
	dl, err := p.fetchDownloadLink(acc, "tr_42", file)
	if err != nil {
		t.Fatalf("fetchDownloadLink error: %v", err)
	}
	if dl.DownloadLink != "http://cdn.example/01.flac" {
		t.Errorf("DownloadLink = %q", dl.DownloadLink)
	}
	if dl.Link != file.Link {
		t.Errorf("cache-key Link = %q, want %q", dl.Link, file.Link)
	}
}

func TestFetchDownloadLink_PlaceholderResolves(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/item/details" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("id"); got != "f1" {
			t.Errorf("item/details id=%q, want f1", got)
		}
		w.Write([]byte(`{"status":"success","id":"f1","name":"01.flac","size":1000,"link":"http://cdn.example/fresh.flac"}`))
	}))
	defer srv.Close()

	p := newTestClient(t, srv.URL)
	acc := p.accountsManager.Current()
	file := &types.File{Id: "f1", Name: "01.flac", Size: 1000, Link: "premiumize://f1"}
	dl, err := p.fetchDownloadLink(acc, "tr_42", file)
	if err != nil {
		t.Fatalf("fetchDownloadLink error: %v", err)
	}
	if dl.DownloadLink != "http://cdn.example/fresh.flac" {
		t.Errorf("DownloadLink = %q, want resolved fresh link", dl.DownloadLink)
	}
}

func TestGetProfile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/account/info" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Write([]byte(`{"status":"success","customer_id":123456,"premium_until":4102444800,"limit_used":0.25}`))
	}))
	defer srv.Close()

	p := newTestClient(t, srv.URL)
	prof, err := p.GetProfile()
	if err != nil {
		t.Fatalf("GetProfile error: %v", err)
	}
	if prof.Username != "123456" {
		t.Errorf("username = %q, want 123456", prof.Username)
	}
	if prof.Expiration.IsZero() {
		t.Error("expected non-zero expiration")
	}
}

func TestCheckFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","response":[false]}`))
	}))
	defer srv.Close()

	p := newTestClient(t, srv.URL)
	err := p.CheckFile(context.Background(), "0000000000000000000000000000000000000000", "")
	if err == nil {
		t.Fatal("expected HosterUnavailableError for uncached repair check")
	}
}

func keys(m map[string]types.File) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
