package utils

import (
	"path/filepath"
	"strings"
)

// mediaExtensions is a set of known media file extensions (lowercase, without dot)
var mediaExtensions = map[string]struct{}{
	// Video
	"webm": {}, "m4v": {}, "3gp": {}, "nsv": {}, "ty": {}, "strm": {},
	"rm": {}, "rmvb": {}, "m3u": {}, "ifo": {}, "mov": {}, "qt": {},
	"divx": {}, "xvid": {}, "bivx": {}, "nrg": {}, "pva": {}, "wmv": {},
	"asf": {}, "asx": {}, "ogm": {}, "ogv": {}, "m2v": {}, "avi": {},
	"bin": {}, "dat": {}, "dvr-ms": {}, "mpg": {}, "mpeg": {}, "mp4": {},
	"avc": {}, "vp3": {}, "svq3": {}, "nuv": {}, "viv": {}, "dv": {},
	"fli": {}, "flv": {}, "wpl": {}, "vob": {}, "mkv": {}, "mk3d": {},
	"ts": {}, "wtv": {}, "m2ts": {},
	// Audio
	"mp2": {}, "mp3": {}, "m4a": {}, "m4b": {}, "m4p": {}, "ogg": {},
	"oga": {}, "opus": {}, "wma": {}, "wav": {}, "wv": {}, "flac": {},
	"ape": {}, "aif": {}, "aiff": {}, "aifc": {},
}

func RemoveInvalidChars(value string) string {
	return strings.Map(func(r rune) rune {
		if r == filepath.Separator || r == ':' {
			return r
		}
		if filepath.IsAbs(string(r)) {
			return r
		}
		if strings.ContainsRune(filepath.VolumeName("C:"+string(r)), r) {
			return r
		}
		if r < 32 || strings.ContainsRune(`<>:"/\|?*`, r) {
			return -1
		}
		return r
	}, value)
}

// SafeJoin joins an untrusted relative path onto base, preserving subdirectories but
// guaranteeing the result stays inside base (any "../" that would escape is dropped).
// Anchoring with a leading "/" before Clean collapses leading "../" segments.
func SafeJoin(base, rel string) string {
	cleaned := filepath.Clean("/" + filepath.ToSlash(rel))
	return filepath.Join(base, filepath.FromSlash(cleaned))
}

// SafePathName reduces an untrusted name (torrent/NZB display name, debrid-returned
// name, uploaded filename) to a SINGLE safe path segment that cannot traverse outside
// its parent directory. It collapses path separators and leading dots so "..", "/etc",
// "..\\x" can never escape the intended download folder.
func SafePathName(value string) string {
	value = strings.ReplaceAll(value, "\\", "/")
	value = strings.ReplaceAll(value, "/", "_")
	value = strings.TrimSpace(value)
	value = strings.TrimLeft(value, ".") // ".."/"."/leading-dot can't hide or escape
	if value == "" {
		return "download"
	}
	return value
}

func RemoveExtension(value string) string {
	ext := filepath.Ext(value)
	if ext == "" {
		return value
	}
	// Remove the leading dot and lowercase for lookup
	extLower := strings.ToLower(ext[1:])
	if _, ok := mediaExtensions[extLower]; ok {
		name := value[:len(value)-len(ext)]
		if name != "" && name != "." {
			return name
		}
	}
	return value
}

func IsMediaFile(path string) bool {
	ext := filepath.Ext(path)
	if ext == "" {
		return false
	}
	extLower := strings.ToLower(ext[1:])
	_, ok := mediaExtensions[extLower]
	return ok
}
