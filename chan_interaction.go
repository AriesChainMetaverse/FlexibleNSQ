package fnsq

var ChanBufferMax = 2048

// interactionChan is an unbounded chan.
// In is used to write without blocking, which supports multiple writers.
// and Out is used to read, wich supports multiple readers.
// You can close the in channel if you want.
type interactionChan struct {
	In     chan<- Interaction // channel for write
	Out    <-chan Interaction // channel for read
	buffer []Interaction      // buffer
}

// Len returns len of Out plus len of buffer.
func (c interactionChan) Len() int {
	return len(c.buffer) + len(c.Out)
}

// BufLen returns len of the buffer.
func (c interactionChan) BufLen() int {
	return len(c.buffer)
}

// NewinteractionChan creates the unbounded chan.
// in is used to write without blocking, which supports multiple writers.
// and out is used to read, wich supports multiple readers.
// You can close the in channel if you want.
func NewinteractionChan(initCapacity int) *interactionChan {
	in := make(chan Interaction, initCapacity)
	out := make(chan Interaction, initCapacity)
	ch := &interactionChan{In: in, Out: out, buffer: make([]Interaction, 0, initCapacity)}

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
						ch.buffer = make([]Interaction, 0, initCapacity)
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
