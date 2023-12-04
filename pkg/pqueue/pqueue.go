package pqueue

import (
	"container/heap"
	"sync"
)

type Item[V any] struct {
	Id       string
	Value    V
	Priority int
	Index    int
}

type MaxItem[V any] []*Item[V]

func (heap MaxItem[V]) Len() int {
	return len(heap)
}

func (heap MaxItem[V]) Less(i, j int) bool {
	return heap[i].Priority > heap[j].Priority
}

func (heap MaxItem[V]) Swap(i, j int) {
	heap[i], heap[j] = heap[j], heap[i]
	heap[i].Index = j
	heap[j].Index = i
}

func (heap *MaxItem[V]) Push(val any) {
	n := len(*heap)
	item := val.(*Item[V])
	item.Index = n
	*heap = append(*heap, item)
}

func (heap *MaxItem[V]) Pop() any {
	old := *heap
	n := len(old)

	item := old[n-1]
	old[n-1] = nil
	item.Index = -1

	*heap = old[0 : n-1]

	return item
}

type PriorityQueue[V any] struct {
	heap MaxItem[V]
	Cond *sync.Cond
}

func New[V any]() *PriorityQueue[V] {
	priorityQueue := PriorityQueue[V]{}
	priorityQueue.heap = make(MaxItem[V], 0)
	priorityQueue.Cond = sync.NewCond(new(sync.Mutex))

	heap.Init(&priorityQueue.heap)

	return &priorityQueue
}

func (queue *PriorityQueue[V]) Push(item *Item[V]) {
	heap.Push(&queue.heap, item)
}

func (queue *PriorityQueue[V]) Pop() *Item[V] {
	return heap.Pop(&queue.heap).(*Item[V])
}

func (queue *PriorityQueue[V]) Peek() *Item[V] {
	return queue.At(0)
}

func (queue *PriorityQueue[V]) Remove(index int) *Item[V] {
	return heap.Remove(&queue.heap, index).(*Item[V])
}

func (queue *PriorityQueue[V]) At(index int) *Item[V] {
	return queue.heap[index]
}

func (queue PriorityQueue[V]) Len() int {
	return queue.heap.Len()
}

func (queue PriorityQueue[V]) IsEmpty() bool {
	return queue.heap.Len() == 0
}

func (queue *PriorityQueue[V]) Id(index int) string {
	return queue.heap[index].Id
}
