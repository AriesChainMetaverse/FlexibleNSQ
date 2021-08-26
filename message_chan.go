package fnsq

var ChanBufferMax = 2048

// MessageChan is an unbounded chan.
// In is used to write without blocking, which supports multiple writers.
// and Out is used to read, wich supports multiple readers.
// You can close the in channel if you want.
type MessageChan struct {
	In     chan<- Publisher // channel for write
	Out    <-chan Publisher // channel for read
	buffer []Publisher      // buffer
}

// Len returns len of Out plus len of buffer.
func (c MessageChan) Len() int {
	return len(c.buffer) + len(c.Out)
}

// BufLen returns len of the buffer.
func (c MessageChan) BufLen() int {
	return len(c.buffer)
}

// NewWorkChan creates the unbounded chan.
// in is used to write without blocking, which supports multiple writers.
// and out is used to read, wich supports multiple readers.
// You can close the in channel if you want.
func NewWorkChan(initCapacity int) *MessageChan {
	in := make(chan Publisher, initCapacity)
	out := make(chan Publisher, initCapacity)
	ch := &MessageChan{In: in, Out: out, buffer: make([]Publisher, 0, initCapacity)}

	go func() {
		defer close(out)
	loop:
		for {
			val, ok := <-in
			if !ok { // in is closed
				break loop
			}

			// out is not full
			select {
			case out <- val:
				continue
			default:
			}

			// out is full
			if len(ch.buffer) < ChanBufferMax {
				ch.buffer = append(ch.buffer, val)
			}
			for len(ch.buffer) > 0 {
				select {
				case val, ok := <-in:
					if !ok { // in is closed
						break loop
					}
					ch.buffer = append(ch.buffer, val)

				case out <- ch.buffer[0]:
					ch.buffer = ch.buffer[1:]
					if len(ch.buffer) == 0 { // after burst
						ch.buffer = make([]Publisher, 0, initCapacity)
					}
				}
			}
		}

		// drain
		for len(ch.buffer) > 0 {
			out <- ch.buffer[0]
			ch.buffer = ch.buffer[1:]
		}
	}()

	return ch
}
