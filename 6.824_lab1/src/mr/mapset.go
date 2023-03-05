//
// @description:map的基本操作：增加、删除、查询有无、查询大小
// @author: Heeler-Deer
// @date: 2023-02-21
// @file: mapset.go
//

package mr

type Mapset struct {
	mapbool map[interface{}]bool
	count   int
}

func NewMapSet() *Mapset {
	m := Mapset{}
	m.mapbool = make(map[interface{}]bool)
	m.count = 0
	return &m
}

func (m *Mapset) Insert(data interface{}) {
	m.count++
	m.mapbool[data] = true
}

func (m *Mapset) Delete(data interface{}) {
	m.count--
	m.mapbool[data] = false
}

func (m *Mapset) Exist(data interface{}) bool {
	return m.mapbool[data]
}
func (m *Mapset) Size() int {
	return m.count
}
