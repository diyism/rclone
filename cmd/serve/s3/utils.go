package s3

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/rclone/gofakes3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/vfs"
)

func getDirEntries(prefix string, VFS *vfs.VFS) (vfs.Nodes, error) {
	// First, try to stat the path as-is
	node, err := VFS.Stat(prefix)

	if err == vfs.ENOENT {
		return nil, gofakes3.ErrNoSuchKey
	} else if err != nil {
		// If we get an error that suggests the path is a file not a directory,
		// this might be because VFS found a .s3 file when we actually want the directory.
		// This can happen when both "foo.s3" (file) and "foo/" (directory) exist.
		// In S3, "foo" can be both an object and a prefix, but in filesystems they conflict.
		// Our solution: files are stored as "foo.s3", directories as "foo/".
		// When VFS traverses "foo", it might find "foo.s3" first and fail.
		// Solution: explicitly request the directory by adding trailing slash.
		errStr := err.Error()
		if strings.Contains(errStr, "file") || strings.Contains(errStr, "directory") ||
			strings.Contains(errStr, "not a directory") {
			// Try to force directory access by ensuring the path ends with /
			dirPath := strings.TrimSuffix(prefix, "/") + "/"
			node, err = VFS.Stat(dirPath)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	if !node.IsDir() {
		return nil, gofakes3.ErrNoSuchKey
	}

	dir := node.(*vfs.Dir)
	dirEntries, err := dir.ReadDirAll()
	if err != nil {
		return nil, err
	}

	return dirEntries, nil
}

func getFileHashByte(node any, hashType hash.Type) []byte {
	b, err := hex.DecodeString(getFileHash(node, hashType))
	if err != nil {
		return nil
	}
	return b
}

func getFileHash(node any, hashType hash.Type) string {
	if hashType == hash.None {
		return ""
	}

	var o fs.Object

	switch b := node.(type) {
	case vfs.Node:
		fsObj, ok := b.DirEntry().(fs.Object)
		if !ok {
			fs.Debugf("serve s3", "File uploading - reading hash from VFS cache")
			in, err := b.Open(os.O_RDONLY)
			if err != nil {
				return ""
			}
			defer func() {
				_ = in.Close()
			}()
			h, err := hash.NewMultiHasherTypes(hash.NewHashSet(hashType))
			if err != nil {
				return ""
			}
			_, err = io.Copy(h, in)
			if err != nil {
				return ""
			}
			return h.Sums()[hashType]
		}
		o = fsObj
	case fs.Object:
		o = b
	}

	hash, err := o.Hash(context.Background(), hashType)
	if err != nil {
		return ""
	}
	return hash
}

func prefixParser(p *gofakes3.Prefix) (path, remaining string) {
	idx := strings.LastIndexByte(p.Prefix, '/')
	if idx < 0 {
		return "", p.Prefix
	}
	return p.Prefix[:idx], p.Prefix[idx+1:]
}

// FIXME this could be implemented by VFS.MkdirAll()
func mkdirRecursive(path string, VFS *vfs.VFS) error {
	path = strings.Trim(path, "/")
	dirs := strings.Split(path, "/")
	dir := ""
	for _, d := range dirs {
		dir += "/" + d
		if _, err := VFS.Stat(dir); err != nil {
			err := VFS.Mkdir(dir, 0777)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func rmdirRecursive(p string, VFS *vfs.VFS) {
	dir := path.Dir(p)
	if !strings.ContainsAny(dir, "/\\") {
		// might be bucket(root)
		return
	}
	if _, err := VFS.Stat(dir); err == nil {
		err := VFS.Remove(dir)
		if err != nil {
			return
		}
		rmdirRecursive(dir, VFS)
	}
}

func authlistResolver(list []string) (map[string]string, error) {
	authList := make(map[string]string)
	for _, v := range list {
		parts := strings.Split(v, ",")
		if len(parts) != 2 {
			return nil, errors.New("invalid auth pair: expecting a single comma")
		}
		authList[parts[0]] = parts[1]
	}
	return authList, nil
}

// toStoragePath converts an S3 object path to a storage path by adding .s3 extension
// This solves the conflict where S3 allows both "foo/bar" (object) and "foo/bar/baz" (object with prefix)
// but hierarchical filesystems like Google Drive don't allow a path to be both a file and directory
// Also URL-encodes each path segment to handle special characters like colons (e.g., "127.0.0.1:13306")
// that are not allowed in Google Drive file/directory names
func toStoragePath(s3Path string) string {
	if s3Path == "" {
		return s3Path
	}

	// Handle directory paths (ending with /)
	isDir := strings.HasSuffix(s3Path, "/")
	if isDir {
		s3Path = strings.TrimSuffix(s3Path, "/")
	}

	// URL encode each path segment (but keep / as separator)
	segments := strings.Split(s3Path, "/")
	for i, seg := range segments {
		if seg != "" {
			segments[i] = url.PathEscape(seg)
		}
	}
	encodedPath := strings.Join(segments, "/")

	// Restore trailing slash for directories
	if isDir {
		return encodedPath + "/"
	}

	// Add .s3 extension for files
	return encodedPath + ".s3"
}

// fromStoragePath converts a storage path back to S3 object path by removing .s3 extension
// and URL-decoding each path segment
func fromStoragePath(storagePath string) string {
	// Remove .s3 extension
	s3Path := strings.TrimSuffix(storagePath, ".s3")

	// URL decode each path segment
	segments := strings.Split(s3Path, "/")
	for i, seg := range segments {
		if seg != "" {
			decoded, err := url.PathUnescape(seg)
			if err == nil {
				segments[i] = decoded
			}
			// If decode fails, keep the original segment
		}
	}

	return strings.Join(segments, "/")
}

// isStorageFile checks if a path represents a file in storage (has .s3 extension)
func isStorageFile(storagePath string) bool {
	return strings.HasSuffix(storagePath, ".s3")
}
