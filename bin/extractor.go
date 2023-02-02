package bin

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"
	"io"

	"go.uber.org/multierr"
)

var errChecksum = errors.New("checksum did not match")

// archiveBinaryExtractor extracts a Kubo binary from a gzipped tar archive from dist.ipfs.io.
type archiveBinaryExtractor struct {
	r io.Reader
	h hash.Hash

	checksum []byte

	gzipReader *gzip.Reader
	tarReader  *tar.Reader
	foundFile  bool
}

func (e *archiveBinaryExtractor) Read(b []byte) (n int, err error) {
	if e.gzipReader == nil {
		gzipReader, err := gzip.NewReader(e.r)
		if err != nil {
			return 0, fmt.Errorf("creating ungzip reader: %w", err)
		}
		e.gzipReader = gzipReader
	}

	if e.tarReader == nil {
		e.tarReader = tar.NewReader(e.gzipReader)
	}

	if !e.foundFile {
		for {
			header, err := e.tarReader.Next()
			if err == io.EOF {
				return 0, errors.New("unable to find binary file in archive")
			}
			if err != nil {
				return 0, fmt.Errorf("error reading tar file: %w", err)
			}
			if header.Typeflag == tar.TypeReg && header.Name == "kubo/ipfs" {
				e.foundFile = true
				break
			}
		}
	}

	n, err = e.tarReader.Read(b)
	if errors.Is(err, io.EOF) {
		// check the digest to make sure the checksum matched
		digest := e.h.Sum(nil)
		if string(digest) != string(e.checksum) {
			fmt.Printf("%q != %q\n", string(digest), string(e.checksum))
			return n, errChecksum
		}
	}
	return n, err
}

func (e *archiveBinaryExtractor) Close() error {
	var merr error
	if c, ok := e.r.(io.Closer); ok {
		multierr.Append(merr, c.Close())
	}
	multierr.Append(merr, e.gzipReader.Close())
	return merr
}

// ExtractKuboBinary extracts the kubo binary from a dist archive,
// also checking the given archive checksum in the process.
func ExtractKuboBinary(r io.Reader, checksum []byte) io.ReadCloser {
	h := sha512.New()
	teeReader := io.TeeReader(r, h)
	return &archiveBinaryExtractor{
		r:        teeReader,
		h:        h,
		checksum: checksum,
	}
}
