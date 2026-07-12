package ydb

import "context"

type contextKey uint8

const readOnlySessionContextKey contextKey = iota

// WithReadOnlySession marks a websocket request as read-only. The session can
// receive room history and updates, but client sync messages are not applied.
func WithReadOnlySession(ctx context.Context) context.Context {
	return context.WithValue(ctx, readOnlySessionContextKey, true)
}

func isReadOnlySession(ctx context.Context) bool {
	readOnly, _ := ctx.Value(readOnlySessionContextKey).(bool)
	return readOnly
}
