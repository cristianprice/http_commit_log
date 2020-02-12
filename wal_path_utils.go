package main

import (
	"fmt"
	"os"
)

//Path is a directory path abstraction.
type Path struct {
	CurrentPath string
}

//AddExtension is a builder method.
func (p *Path) AddExtension(ext string) *Path {
	ret := &Path{
		CurrentPath: fmt.Sprint(p.CurrentPath, ext),
	}
	return ret
}

//AddUint32 is a builder method.
func (p *Path) AddUint32(child uint32) *Path {
	return p.Add(fmt.Sprint(child))
}

//AddInt64 is a builder method.
func (p *Path) AddInt64(child int64) *Path {
	return p.Add(fmt.Sprint(child))
}

//Add is a builder method.
func (p *Path) Add(child string) *Path {
	ret := &Path{
		CurrentPath: fmt.Sprint(p.CurrentPath, string(os.PathSeparator), child),
	}
	return ret
}
