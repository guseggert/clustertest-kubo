package bin

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"

	"go.uber.org/multierr"
)

// archiveBinaryExtractor extracts a Kubo binary from a gzipped tar archive from dist.ipfs.io.
type archiveBinaryExtractor struct {
	r io.Reader

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
	return e.tarReader.Read(b)
}

func (e *archiveBinaryExtractor) Close() error {
	var merr error
	if c, ok := e.r.(io.Closer); ok {
		multierr.Append(merr, c.Close())
	}
	multierr.Append(merr, e.gzipReader.Close())
	return merr
}

// ExtractKuboBinary extracts the kubo binary from a dist archive.
func ExtractKuboBinary(r io.Reader) io.ReadCloser {
	return &archiveBinaryExtractor{r: r}
}
