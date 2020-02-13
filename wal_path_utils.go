package main

import (
	"fmt"
	"os"
)

//Path is a directory path abstraction.
type Path string

//AddExtension is a builder method.
func (p *Path) AddExtension(ext string) *Path {
	ret := Path(fmt.Sprint(p, ext))
	return &ret
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
	ret := Path(fmt.Sprint(p, string(os.PathSeparator), child))
	return &ret
}

func (p *Path) String() string {
	return string(*p)
}
