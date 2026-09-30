package sdk

type Behavior struct{}

type SystemSettings struct{}

type BehaviorPatches struct{}

func DefaultBehavior() *Behavior {
	return &Behavior{}
}

func NewBehavior(name string, patches BehaviorPatches, parent *Behavior) *Behavior {
	return &Behavior{}
}

func ReadFastBehavior() *Behavior {
	return &Behavior{}
}

func StrictlyConsistentBehavior() *Behavior {
	return &Behavior{}
}

func FastRackAwareBehavior() *Behavior {
	return &Behavior{}
}

func (b *Behavior) Explain() string {
	return ""
}
