package sftp

type (
	bufPool struct {
		ch   chan []byte
		blen int
	}

	resChanPool chan chan result
)

func newBufPool(depth, bufLen int) *bufPool {
	return &bufPool{
		ch:   make(chan []byte, depth),
		blen: bufLen,
	}
}

func (p *bufPool) Get() []byte {
	if p.blen <= 0 {
		panic("bufPool: new buffer creation length must be greater than zero")
	}

	for {
		select {
		case b := <-p.ch:
			if cap(b) < p.blen {
				continue
			}

			return b[:p.blen]

		default:
			return make([]byte, p.blen)
		}
	}
}

func (p *bufPool) Put(b []byte) {
	if p == nil {
		return
	}

	if cap(b) < p.blen || cap(b) > p.blen*2 {
		return
	}

	select {
	case p.ch <- b:
	default:
	}
}

func newResChanPool(depth int) resChanPool {
	return make(chan chan result, depth)
}

func (p resChanPool) Get() chan result {
	select {
	case ch := <-p:
		return ch
	default:
		return make(chan result, 1)
	}
}

func (p resChanPool) Put(ch chan result) {
	select {
	case p <- ch:
	default:
	}
}
