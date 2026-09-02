package sftp

import (
	"sync"
)

type allocator struct {
	sync.Mutex
	available [][]byte
	// map key is the request order
	used map[uint32]pageSet
}

// pageSet keeps the pages used by the common receive+transfer path inline.
// This avoids allocating a short []byte slice for every request. Requests that
// need more than two pages transparently fall back to overflow.
type pageSet struct {
	inline   [2][]byte
	count    int
	overflow [][]byte
}

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
		// micro optimization: initialize available pages with an initial capacity
		available: make([][]byte, 0, SftpServerWorkerCount*2),
		used:      make(map[uint32]pageSet),
	}
}

// GetPage returns a previously allocated and unused []byte or create a new one.
// The slice have a fixed size = maxMsgLength, this value is suitable for both
// receiving new packets and reading the files to serve
func (a *allocator) GetPage(requestOrderID uint32) []byte {
	a.Lock()

	var result []byte

	// get an available page and remove it from the available ones.
	if len(a.available) > 0 {
		truncLength := len(a.available) - 1
		result = a.available[truncLength]

		a.available[truncLength] = nil          // clear out the internal pointer
		a.available = a.available[:truncLength] // truncate the slice
	}

	// no preallocated slice found, just allocate a new one
	if result == nil {
		result = make([]byte, maxMsgLength)
	}

	// put result in used pages
	pages := a.used[requestOrderID]
	pages.add(result)
	a.used[requestOrderID] = pages
	a.Unlock()

	return result
}

// ReleasePages marks unused all pages in use for the given requestID
func (a *allocator) ReleasePages(requestOrderID uint32) {
	a.Lock()

	if used, ok := a.used[requestOrderID]; ok {
		a.available = append(a.available, used.inline[:used.count]...)
		a.available = append(a.available, used.overflow...)
		delete(a.used, requestOrderID)
	}
	a.Unlock()
}

// Free removes all the used and available pages.
// Call this method when the allocator is not needed anymore
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
