package queryexamples

import (
	"context"
	"fmt"
	"strings"
)

// ListNamespaces returns the names of every namespace on the cluster.
//
// DX GAP: Session.Info returns a raw map[string]string —
// {command: raw response text}, matching the root package's own
// RequestInfo convention (connection.go) — not a typed result. Java's
// equivalent (session.info().namespaces()) hands back a parsed
// Set<String> directly; here the customer has to know the wire format (a
// semicolon-separated list in the raw response string) and parse it
// themselves.
func (s *Service) ListNamespaces(ctx context.Context) ([]string, error) {
	info, err := s.session.Info(ctx, "namespaces")
	if err != nil {
		return nil, fmt.Errorf("list namespaces: %w", err)
	}
	raw, ok := info["namespaces"]
	if !ok || raw == "" {
		return nil, nil
	}
	var namespaces []string
	for _, ns := range strings.Split(strings.TrimSuffix(raw, "\n"), ";") {
		if ns != "" {
			namespaces = append(namespaces, ns)
		}
	}
	return namespaces, nil
}

// NamespaceStats returns the parsed key=value stats for one namespace,
// from the "namespace/<name>" info command.
//
// DX GAP: same root cause as ListNamespaces — this hand-parses a
// semicolon/equals-delimited blob because Session.Info returns raw text,
// not a typed NamespaceDetail the way Java's
// session.info().namespaceDetails(ns) does. Secondary-index listing
// (session.info().secondaryIndexes() in Java) is left for a follow-up
// slice rather than guessed at here — its wire format nests a second
// delimiter layer (colon-separated index blocks, each itself
// semicolon/equals pairs) that isn't verified against a real server in
// this pass.
func (s *Service) NamespaceStats(ctx context.Context, namespace string) (map[string]string, error) {
	command := "namespace/" + namespace
	info, err := s.session.Info(ctx, command)
	if err != nil {
		return nil, fmt.Errorf("get namespace stats for %s: %w", namespace, err)
	}
	raw, ok := info[command]
	if !ok {
		return nil, fmt.Errorf("get namespace stats for %s: no response for command %q", namespace, command)
	}

	stats := make(map[string]string)
	for _, pair := range strings.Split(strings.TrimSuffix(raw, "\n"), ";") {
		if pair == "" {
			continue
		}
		key, value, found := strings.Cut(pair, "=")
		if !found {
			continue
		}
		stats[key] = value
	}
	return stats, nil
}
