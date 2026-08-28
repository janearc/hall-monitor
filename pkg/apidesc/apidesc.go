// Package apidesc serves the binary's API descriptor at /api: a
// FileDescriptorSet built from the RUNTIME protoregistry, so the served spec
// is derived from the same bytes the linked types came from and cannot drift
// from them -- the flipr /api property, without the build-time embed. It also
// answers the versioning directive's surface question: the descriptor lists
// every major this binary speaks (truth.v1, lease.v1, ...), which is the
// "speaks" column read mechanically.
package apidesc

import (
	"net/http"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Handler serves the FileDescriptorSet, binary proto, deterministically
// ordered by file path so two requests are byte-comparable.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var files []*descriptorpb.FileDescriptorProto
		protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
			files = append(files, protodesc.ToFileDescriptorProto(fd))
			return true
		})
		sort.Slice(files, func(i, j int) bool { return files[i].GetName() < files[j].GetName() })
		b, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: files})
		if err != nil {
			http.Error(w, "descriptor marshal failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(b)
	})
}
