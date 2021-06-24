/*
 * JuiceFS, Copyright (C) 2018 Juicedata, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package object

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/juicedata/juicefs/pkg/meta"
)

type withOwner struct {
	os ObjectStorage
	m  meta.Meta
}

// withOwner retuns a object storage that add a prefix to keys.
func WithOwnerPrefix(os ObjectStorage, m meta.Meta) ObjectStorage {
	return &withOwner{os, m}
}

// key: chunks/0/0/1_0_4194304
func (p *withOwner) getOwner(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) <= 1 {
		logger.Errorf("Uknonw chunk key path format: %s", key)
		return ""
	}
	parts = strings.Split(parts[len(parts)-1], "_")
	if len(parts) != 3 {
		logger.Errorf("Uknonw chunk key id format: %s", key)
		return ""
	}
	chunkid, _ := strconv.ParseUint(parts[0], 10, 64)
	return p.m.GetChunkOwner(chunkid) + "/"
}

func (p *withOwner) String() string {
	return fmt.Sprintf("%s[owner]", p.os)
}

func (p *withOwner) Create() error {
	return p.os.Create()
}

func (p *withOwner) Head(key string) (Object, error) {
	o, err := p.os.Head(p.getOwner((key)) + key)
	if err != nil {
		return nil, err
	}
	switch po := o.(type) {
	case *obj:
		po.key = po.key[len(p.getOwner(key)):]
	case *file:
		po.key = po.key[len(p.getOwner(key)):]
	}
	return o, nil
}

func (p *withOwner) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return p.os.Get(p.getOwner(key)+key, off, limit)
}

func (p *withOwner) Put(key string, in io.Reader) error {
	return p.os.Put(p.getOwner(key)+key, in)
}

func (p *withOwner) Delete(key string) error {
	return p.os.Delete(p.getOwner(key) + key)
}

func (p *withOwner) List(prefix, marker string, limit int64) ([]Object, error) {
	if marker != "" {
		marker = p.getOwner(marker) + marker
	}
	objs, err := p.os.List(p.getOwner(marker)+prefix, marker, limit)
	ln := len(p.getOwner(prefix))
	for _, o := range objs {
		switch p := o.(type) {
		case *obj:
			p.key = p.key[ln:]
		case *file:
			p.key = p.key[ln:]
		}
	}
	return objs, err
}

func (p *withOwner) ListAll(prefix, marker string) (<-chan Object, error) {
	if marker != "" {
		marker = p.getOwner(marker) + marker
	}
	r, err := p.os.ListAll(p.getOwner(marker)+prefix, marker)
	if err != nil {
		return r, err
	}
	r2 := make(chan Object, 10240)
	ln := len(p.getOwner(marker))
	go func() {
		for o := range r {
			if o != nil {
				switch p := o.(type) {
				case *obj:
					p.key = p.key[ln:]
				case *file:
					p.key = p.key[ln:]
				}
			}
			r2 <- o
		}
		close(r2)
	}()
	return r2, nil
}

func (p *withOwner) Chmod(path string, mode os.FileMode) error {
	if fs, ok := p.os.(FileSystem); ok {
		return fs.Chmod(p.getOwner(path)+path, mode)
	}
	return nil
}

func (p *withOwner) Chown(path string, owner, group string) error {
	if fs, ok := p.os.(FileSystem); ok {
		return fs.Chown(p.getOwner(path)+path, owner, group)
	}
	return nil
}

func (p *withOwner) CreateMultipartUpload(key string) (*MultipartUpload, error) {
	return p.os.CreateMultipartUpload(p.getOwner(key) + key)
}

func (p *withOwner) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	return p.os.UploadPart(p.getOwner(key)+key, uploadID, num, body)
}

func (p *withOwner) AbortUpload(key string, uploadID string) {
	p.os.AbortUpload(p.getOwner(key)+key, uploadID)
}

func (p *withOwner) CompleteUpload(key string, uploadID string, parts []*Part) error {
	return p.os.CompleteUpload(p.getOwner(key)+key, uploadID, parts)
}

func (p *withOwner) ListUploads(marker string) ([]*PendingPart, string, error) {
	parts, nextMarker, err := p.os.ListUploads(marker)
	for _, part := range parts {
		part.Key = part.Key[len(p.getOwner(marker)):]
	}
	return parts, nextMarker, err
}

var _ ObjectStorage = &withOwner{}
