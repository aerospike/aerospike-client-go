package sdk

// InfoCommands holds convenience wrappers over Session.Info for common
// commands (e.g. Stats()). NOTE: as specified in the PRD, neither
// Session.InfoCommands() nor Stats() takes a ctx, yet Stats() is a
// network-touching call — this is a known open gap (see D-1), not an
// oversight in this stub.
type InfoCommands struct{}

func (c *InfoCommands) Stats() (map[string]string, error) {
	return nil, nil
}
