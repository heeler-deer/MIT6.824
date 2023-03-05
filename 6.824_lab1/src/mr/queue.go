//
// @description: 队列的基本操作
// @author: Heeler-Deer
// @date: 2023-02-21
// @file: queue.go
//

package mr

import (
	"errors"
	"sync"
)

//
// @description:链表的节点，包括在前后增加删除四种操作
// param:
// return:
//

type ListNode struct {
	data interface{}
	prev *ListNode
	next *ListNode
}

func (node *ListNode) AddBefore(data interface{}) {
	prev := node.prev
	newNode := ListNode{}
	newNode.data = data
	newNode.next = node
	node.prev = &newNode
	newNode.prev = prev
	prev.next = &newNode
}

func (node *ListNode) AddAfter(data interface{}) {
	next := node.next

	newnode := ListNode{}
	newnode.data = data

	newnode.prev = node
	node.next = &newnode
	newnode.next = next
	next.prev = &newnode
}

func (node *ListNode) DelBefore() {
	prev := node.prev.prev
	node.prev = prev
	prev.next = node

}

func (node *ListNode) DelAfter() {
	next := node.next.next
	node.next = next
	next.prev = node
}

//
// @description:链表，包括新建节点、查询大小、前加、后加、前删、后删、查询前数据、查询后数据
// param:
// return:
//

type List struct {
	head  ListNode
	count int
}

func NewList() *List {
	list := List{}
	list.head.next = &list.head
	list.head.prev = &list.head
	list.count = 0
	return &list
}

func (list *List) size() int {
	return list.count
}

func (list *List) AddBefore(data interface{}) {
	list.head.AddAfter(data)
	list.count++
}
func (list *List) AddAfter(data interface{}) {
	list.head.AddBefore(data)
	list.count++
}

// pop函数参见c++ stl_list实现
func (list *List) PopFront() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("DelBefore failed!")
	}
	data := list.head.next.data
	list.head.DelAfter()
	list.count--
	return data, nil
}

func (list *List) PopBack() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("DelAfter failed!")

	}

	data := list.head.prev.data
	list.head.DelBefore()
	list.count--

	return data, nil
}

func (list *List) PickBefore() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("PickBefore failed!")
	}
	return list.head.next.data, nil
}

func (list *List) PickAfter() (interface{}, error) {
	if list.count == 0 {
		return nil, errors.New("PickAfter failed!")
	}
	return list.head.prev.data, nil
}

// @description:消息阻塞队列,包括新建、查询大小、前加、后加、前删、后删、查询前数据、查询后数据
// param:
// return:
type BlockQueue struct {
	list *List
	cond *sync.Cond
}

func NewBlockQueue() *BlockQueue {
	queue := BlockQueue{}
	queue.list = NewList()
	queue.cond = sync.NewCond(new(sync.Mutex))
	return &queue
}

func (bq *BlockQueue) size() int {
	bq.cond.L.Lock()
	ret := bq.list.size()
	bq.cond.L.Unlock()
	return ret
}

func (bq *BlockQueue) AddBefore(data interface{}) {
	bq.cond.L.Lock()
	bq.list.AddBefore(data)
	bq.cond.L.Unlock()
	bq.cond.Broadcast()
}

func (bq *BlockQueue) AddAfter(data interface{}) {
	bq.cond.L.Lock()
	bq.list.AddAfter(data)
	bq.cond.L.Unlock()
	bq.cond.Broadcast()
}

func (bq *BlockQueue) PopFront() (interface{}, error) {
	bq.cond.L.Lock()
	data, er := bq.list.PopFront()
	bq.cond.L.Unlock()
	return data, er
}

func (bq *BlockQueue) PopBack() (interface{}, error) {
	bq.cond.L.Lock()
	data, er := bq.list.PopBack()
	bq.cond.L.Unlock()
	return data, er
}

func (bq *BlockQueue) PickBefore() (interface{}, error) {
	bq.cond.L.Lock()
	for bq.list.count == 0 {
		bq.cond.Wait()
	}
	data, err := bq.list.PickBefore()
	bq.cond.L.Unlock()
	return data, err
}
func (bq *BlockQueue) PickAfter() (interface{}, error) {
	bq.cond.L.Lock()
	for bq.list.count == 0 {
		bq.cond.Wait()
	}
	data, err := bq.list.PickAfter()
	bq.cond.L.Unlock()
	return data, err
}
