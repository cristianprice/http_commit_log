package main

import (
	"fmt"
	"os"
	"path/filepath"
)

//Path is a directory path abstraction.
type Path string

//FromString converts string to Path
func FromString(path string) *Path {
	if !filepath.IsAbs(path) {
		panic(fmt.Sprint(path, " is not an absolute path."))
	}
	ret := Path(path)
	return &ret
}

//BaseDir returns the parent directory.
func (p Path) BaseDir() Path {
	ret := Path(filepath.Dir(string(p)))
	return ret
}

//AddExtension is a builder method.
func (p Path) AddExtension(ext string) Path {
	ret := Path(fmt.Sprint(p, ext))
	return ret
}

//AddUint32 is a builder method.
func (p Path) AddUint32(child uint32) Path {
	return p.Add(fmt.Sprint(child))
}

//AddInt64 is a builder method.
func (p Path) AddInt64(child int64) Path {
	return p.Add(fmt.Sprint(child))
}

//Add is a builder method.
func (p Path) Add(child string) Path {
	ret := Path(fmt.Sprint(p, string(os.PathSeparator), child))
	return ret
}

func (p Path) String() string {
	return string(p)
}
