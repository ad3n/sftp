package sftp

import (
	"sync"
)

type (
	allocator struct {
		used      map[uint32]pageSet
		available [][]byte

		sync.Mutex
	}

	pageSet struct {
		inline   [2][]byte
		overflow [][]byte
		count    int
	}
)

func (p *pageSet) add(page []byte) {
	if p.count < len(p.inline) {
		p.inline[p.count] = page
		p.count++
		return
	}

	p.overflow = append(p.overflow, page)
}

func (p *pageSet) len() int {
	return p.count + len(p.overflow)
}

func newAllocator() *allocator {
	return &allocator{
		available: make([][]byte, 0, SftpServerWorkerCount*2),
		used:      make(map[uint32]pageSet),
	}
}

func (a *allocator) GetPage(requestOrderID uint32) []byte {
	a.Lock()

	var result []byte

	if len(a.available) > 0 {
		truncLength := len(a.available) - 1
		result = a.available[truncLength]

		a.available[truncLength] = nil
		a.available = a.available[:truncLength]
	}

	if result == nil {
		result = make([]byte, maxMsgLength)
	}

	pages := a.used[requestOrderID]
	pages.add(result)
	a.used[requestOrderID] = pages
	a.Unlock()

	return result
}

func (a *allocator) ReleasePages(requestOrderID uint32) {
	a.Lock()

	if used, ok := a.used[requestOrderID]; ok {
		a.available = append(a.available, used.inline[:used.count]...)
		a.available = append(a.available, used.overflow...)
		delete(a.used, requestOrderID)
	}

	a.Unlock()
}

func (a *allocator) Free() {
	a.Lock()
	defer a.Unlock()

	a.available = nil
	a.used = make(map[uint32]pageSet)
}

func (a *allocator) countUsedPages() int {
	a.Lock()
	defer a.Unlock()

	num := 0
	for _, p := range a.used {
		num += p.len()
	}

	return num
}

func (a *allocator) countAvailablePages() int {
	a.Lock()
	defer a.Unlock()

	return len(a.available)
}

func (a *allocator) isRequestOrderIDUsed(requestOrderID uint32) bool {
	a.Lock()
	defer a.Unlock()

	_, ok := a.used[requestOrderID]
	return ok
}
