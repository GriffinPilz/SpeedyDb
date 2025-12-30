package btree

import "sort"

type Row map[string]any

type Item struct {
	PK  int
	Row Row
}

type BTree struct {
	t    int
	root *node
	n    int
}

type node struct {
	leaf     bool
	items    []Item
	children []*node // len(children) = len(items)+1 when non-leaf
}

func New(t int) *BTree {
	if t < 2 {
		t = 2
	}
	return &BTree{
		t:    t,
		root: &node{leaf: true},
		n:    0,
	}
}

type Iter struct {
	stack []iterFrame
}

type iterFrame struct {
	n *node
	i int
}

func (tr *BTree) IterAscend() *Iter {
	it := &Iter{}
	it.pushLeft(tr.root)
	return it
}

// Next returns the next Item in ascending PK order.
// ok=false when iteration is finished.
func (it *Iter) Next() (item Item, ok bool) {
	for len(it.stack) > 0 {
		top := &it.stack[len(it.stack)-1]
		n := top.n
		i := top.i

		// For internal nodes, before emitting item i we must have already
		// fully processed child i (handled by pushLeft when descending).
		if i < len(n.items) {
			// Emit item i
			item = n.items[i]
			top.i++

			// After emitting item i, traverse leftmost path of child i+1 (if exists)
			if !n.leaf {
				it.pushLeft(n.children[i+1])
			}
			return item, true
		}

		// Done with this node
		it.stack = it.stack[:len(it.stack)-1]
	}
	return Item{}, false
}

func (it *Iter) pushLeft(n *node) {
	for n != nil {
		it.stack = append(it.stack, iterFrame{n: n, i: 0})
		if n.leaf {
			return
		}
		n = n.children[0]
	}
}

// Get returns the Row for pk if present.
func (tr *BTree) Get(pk int) (Row, bool) {
	n := tr.root
	for {
		i := sort.Search(len(n.items), func(i int) bool { return n.items[i].PK >= pk })
		if i < len(n.items) && n.items[i].PK == pk {
			return n.items[i].Row, true
		}
		if n.leaf {
			return nil, false
		}
		n = n.children[i]
	}
}

// Upsert inserts item or replaces existing. Returns (old, replaced).
func (tr *BTree) Upsert(it Item) (Item, bool) {
	r := tr.root
	if len(r.items) == 2*tr.t-1 {
		s := &node{leaf: false, children: []*node{r}}
		tr.splitChild(s, 0)
		tr.root = s
		old, replaced := tr.insertNonFull(s, it)
		if !replaced {
			tr.n++
		}
		return old, replaced
	}

	old, replaced := tr.insertNonFull(r, it)
	if !replaced {
		tr.n++
	}
	return old, replaced
}

func (tr *BTree) Len() int {
	return tr.n
}

func (tr *BTree) IsEmpty() bool {
	return tr.n == 0
}

func (tr *BTree) insertNonFull(n *node, it Item) (Item, bool) {
	// Find first index with PK >= it.PK
	i := sort.Search(len(n.items), func(i int) bool { return n.items[i].PK >= it.PK })

	if n.leaf {
		// Replace if exists
		if i < len(n.items) && n.items[i].PK == it.PK {
			old := n.items[i]
			n.items[i] = it
			return old, true
		}
		// Insert into items at i
		n.items = append(n.items, Item{})
		copy(n.items[i+1:], n.items[i:])
		n.items[i] = it
		return Item{}, false
	}

	// Internal node: if key exists in internal node, replace there.
	if i < len(n.items) && n.items[i].PK == it.PK {
		old := n.items[i]
		n.items[i] = it
		return old, true
	}

	// Ensure child is not full before descending.
	if len(n.children[i].items) == 2*tr.t-1 {
		tr.splitChild(n, i)
		// After split, decide which child to go into.
		if it.PK > n.items[i].PK {
			i++
		} else if it.PK == n.items[i].PK {
			old := n.items[i]
			n.items[i] = it
			return old, true
		}
	}
	return tr.insertNonFull(n.children[i], it)
}

// splitChild splits n.children[i] (which must be full) into two nodes and
// moves the median item up into n.items[i].
func (tr *BTree) splitChild(n *node, i int) {
	t := tr.t
	y := n.children[i]       // full child
	z := &node{leaf: y.leaf} // new node

	// Median item to move up: y.items[t-1]
	median := y.items[t-1]

	// z gets y's last (t-1) items
	z.items = append(z.items, y.items[t:]...)
	// y keeps first (t-1) items
	y.items = y.items[:t-1]

	// If not leaf, split children too: y has 2t children, keep t, move t to z
	if !y.leaf {
		z.children = append(z.children, y.children[t:]...)
		y.children = y.children[:t]
	}

	// Insert median into n.items at position i
	n.items = append(n.items, Item{})
	copy(n.items[i+1:], n.items[i:])
	n.items[i] = median

	// Insert z as child right after y
	n.children = append(n.children, nil)
	copy(n.children[i+2:], n.children[i+1:])
	n.children[i+1] = z
}

// AscendRange calls fn for items with PK in [lo, hi] in sorted order.
// If fn returns false, iteration stops early.
func (tr *BTree) AscendRange(lo, hi int, fn func(Item) bool) {
	ascendRangeNode(tr.root, lo, hi, fn)
}

func ascendRangeNode(n *node, lo, hi int, fn func(Item) bool) bool {
	// In-order traversal:
	// child0, item0, child1, item1, ..., childK
	for i := 0; i < len(n.items); i++ {
		if !n.leaf {
			if !ascendRangeNode(n.children[i], lo, hi, fn) {
				return false
			}
		}
		pk := n.items[i].PK
		if pk >= lo && pk < hi {
			if !fn(n.items[i]) {
				return false
			}
		}
		// small pruning: if pk > hi, we can stop early
		if pk >= hi {
			// still need to stop without visiting further children/items
			if !n.leaf {
				return false
			}
			return false
		}
	}
	if !n.leaf {
		return ascendRangeNode(n.children[len(n.items)], lo, hi, fn)
	}
	return true
}
