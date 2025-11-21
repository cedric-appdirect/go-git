package filesystem

import (
	"io"
	"os"
	"path"
	"time"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/utils/convert"
	"github.com/go-git/go-git/v6/utils/ioutil"
	"github.com/go-git/go-git/v6/utils/merkletrie/noder"
	"github.com/go-git/go-git/v6/utils/sync"

	"github.com/go-git/go-billy/v6"
)

var ignore = map[string]bool{
	".git": true,
}

type Options struct {
	// AutoCRLF converts CRLF line endings in text files into LF line endings.
	AutoCRLF bool
}

// The node represents a file or a directory in a billy.Filesystem. It
// implements the interface noder.Noder of merkletrie package.
//
// This implementation implements a "standard" hash method being able to be
// compared with any other noder.Noder implementation inside of go-git.
type node struct {
	fs         billy.Filesystem
	submodules map[string]plumbing.Hash
	idx        *index.Index           // for metadata comparison optimization
	idxMap     map[string]*index.Entry // cached index entries for O(1) lookup

	options *Options

	path     string
	hash     []byte
	children []noder.Noder
	isDir    bool
	mode     os.FileMode
	size     int64
	modTime  time.Time // cached from ReadDir to avoid extra Lstat
}

// NewRootNode returns the root node based on a given billy.Filesystem.
//
// In order to provide the submodule hash status, a map[string]plumbing.Hash
// should be provided where the key is the path of the submodule and the commit
// of the submodule HEAD
func NewRootNode(
	fs billy.Filesystem,
	submodules map[string]plumbing.Hash,
) noder.Noder {
	return &node{fs: fs, submodules: submodules, isDir: true}
}

func NewRootNodeWithOptions(
	fs billy.Filesystem,
	submodules map[string]plumbing.Hash,
	options Options,
) noder.Noder {
	return &node{
		fs:         fs,
		submodules: submodules,
		options:    &options,
		isDir:      true,
	}
}

// NewRootNodeWithIndex returns the root node based on a given billy.Filesystem
// and an index. This enables the metadata-first comparison optimization where
// we check file metadata (mtime, size, mode) against the index before hashing.
//
// This dramatically improves Status() performance by avoiding unnecessary file
// I/O when files haven't changed.
func NewRootNodeWithIndex(
	fs billy.Filesystem,
	submodules map[string]plumbing.Hash,
	idx *index.Index,
	options Options,
) noder.Noder {
	// Build a map of index entries for O(1) lookup
	// This avoids the O(n) linear search in Index.Entry()
	idxMap := make(map[string]*index.Entry, len(idx.Entries))
	for _, entry := range idx.Entries {
		idxMap[entry.Name] = entry
	}

	return &node{
		fs:         fs,
		submodules: submodules,
		idx:        idx,
		idxMap:     idxMap,
		options:    &options,
		isDir:      true,
	}
}

// Hash the hash of a filesystem is the result of concatenating the computed
// plumbing.Hash of the file as a Blob and its plumbing.FileMode; that way the
// difftree algorithm will detect changes in the contents of files and also in
// their mode.
//
// Please note that the hash is calculated on first invocation of Hash(),
// meaning that it will not update when the underlying file changes
// between invocations.
//
// The hash of a directory is always a 24-bytes slice of zero values
func (n *node) Hash() []byte {
	if n.hash == nil {
		n.calculateHash()
	}
	return n.hash
}

func (n *node) Name() string {
	return path.Base(n.path)
}

func (n *node) IsDir() bool {
	return n.isDir
}

func (n *node) Skip() bool {
	return false
}

func (n *node) Children() ([]noder.Noder, error) {
	if err := n.calculateChildren(); err != nil {
		return nil, err
	}

	return n.children, nil
}

func (n *node) NumChildren() (int, error) {
	if err := n.calculateChildren(); err != nil {
		return -1, err
	}

	return len(n.children), nil
}

func (n *node) calculateChildren() error {
	if !n.IsDir() {
		return nil
	}

	if len(n.children) != 0 {
		return nil
	}

	files, err := n.fs.ReadDir(n.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, file := range files {
		if _, ok := ignore[file.Name()]; ok {
			continue
		}

		fi, err := file.Info()
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSocket != 0 {
			continue
		}

		c, err := n.newChildNode(fi)
		if err != nil {
			return err
		}

		n.children = append(n.children, c)
	}

	return nil
}

func (n *node) newChildNode(file os.FileInfo) (*node, error) {
	path := path.Join(n.path, file.Name())

	node := &node{
		fs:         n.fs,
		submodules: n.submodules,
		idx:        n.idx,
		idxMap:     n.idxMap,
		options:    n.options,

		path:    path,
		isDir:   file.IsDir(),
		size:    file.Size(),
		mode:    file.Mode(),
		modTime: file.ModTime(), // Cache modtime from ReadDir
	}

	if _, isSubmodule := n.submodules[path]; isSubmodule {
		node.isDir = false
	}

	return node, nil
}

func (n *node) calculateHash() {
	if n.isDir {
		n.hash = make([]byte, 24)
		return
	}
	mode, err := filemode.NewFromOSFileMode(n.mode)
	if err != nil {
		n.hash = plumbing.ZeroHash.Bytes()
		return
	}
	if submoduleHash, isSubmodule := n.submodules[n.path]; isSubmodule {
		n.hash = append(submoduleHash.Bytes(), filemode.Submodule.Bytes()...)
		return
	}

	// Optimization: Check metadata before hashing.
	// If the file's metadata (mtime, size, mode) matches the index entry,
	// we can reuse the hash from the index instead of reading and hashing
	// the file content. This is the same optimization that native git uses
	// (ie_match_stat in read-cache.c) and dramatically improves Status()
	// performance by avoiding file I/O for unchanged files.
	if entry := n.getIndexEntry(); entry != nil && n.metadataMatches(entry) {
		// Metadata matches - reuse hash from index
		n.hash = append(entry.Hash.Bytes(), mode.Bytes()...)
		return
	}

	// Metadata differs or no index entry - hash the file
	var hash plumbing.Hash
	if n.mode&os.ModeSymlink != 0 {
		hash = n.doCalculateHashForSymlink()
	} else {
		hash = n.doCalculateHashForRegular()
	}
	n.hash = append(hash.Bytes(), mode.Bytes()...)
}

// getIndexEntry retrieves the index entry for this file, if available.
// Uses the O(1) map lookup instead of O(n) linear search.
func (n *node) getIndexEntry() *index.Entry {
	if n.idxMap == nil {
		return nil
	}
	return n.idxMap[n.path]
}

// metadataMatches checks if the file's metadata matches the given index entry.
// This implements the same optimization as git's ie_match_stat() function.
// Returns true if the file appears unchanged based on metadata, false otherwise.
func (n *node) metadataMatches(entry *index.Entry) bool {
	// Size check (fastest check, catches most changes)
	if uint32(n.size) != entry.Size {
		return false
	}

	// Modification time check
	// Use the cached modTime from ReadDir - no extra Lstat needed!
	// Use Equal() to handle sub-second precision correctly across platforms
	if !n.modTime.IsZero() && !n.modTime.Equal(entry.ModifiedAt) {
		return false
	}

	// Mode check
	mode, err := filemode.NewFromOSFileMode(n.mode)
	if err != nil {
		return false
	}

	// Compare modes - for executable files, check if the index also has executable
	if mode != entry.Mode {
		return false
	}

	return true
}

func (n *node) doCalculateHashForRegular() plumbing.Hash {
	f, err := n.fs.Open(n.path)
	if err != nil {
		return plumbing.ZeroHash
	}
	defer f.Close()

	h := plumbing.NewHasher(format.SHA1, plumbing.BlobObject, n.size)
	var dst io.Writer = h

	if n.options != nil && n.options.AutoCRLF {
		br := sync.GetBufioReader(f)
		defer sync.PutBufioReader(br)

		stat, err := convert.GetStat(br)
		if err != nil {
			return plumbing.ZeroHash
		}

		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return plumbing.ZeroHash
		}

		if !stat.IsBinary() {
			h.Reset(plumbing.BlobObject, n.size-int64(stat.CRLF))
			dst = convert.NewLFWriter(dst)
		}
	}

	if _, err := ioutil.CopyBufferPool(dst, f); err != nil {
		return plumbing.ZeroHash
	}

	return h.Sum()
}

func (n *node) doCalculateHashForSymlink() plumbing.Hash {
	target, err := n.fs.Readlink(n.path)
	if err != nil {
		return plumbing.ZeroHash
	}

	h := plumbing.NewHasher(format.SHA1, plumbing.BlobObject, n.size)
	if _, err := h.Write([]byte(target)); err != nil {
		return plumbing.ZeroHash
	}

	return h.Sum()
}

func (n *node) String() string {
	return n.path
}
