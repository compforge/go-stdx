package model

// Key separates ID and name namespaces. Name is descriptive when ID exists.
// The empty key identifies the default executor; it is distinct from named actors.
func (a Actor) Key() string {
	if a.ID != "" {
		return "id:" + a.ID
	}
	if a.Name != "" {
		return "name:" + a.Name
	}
	return ""
}

// StageKey identifies one actor's contribution to a logical stage.
type StageKey struct {
	ID    StageID
	Actor string
}

func (s Stage) Key() StageKey { return StageKey{ID: s.ID, Actor: s.Actor.Key()} }
