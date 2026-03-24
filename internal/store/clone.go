package store

func cloneMessage(msg Message) Message {
	clone := msg
	if msg.Body != nil {
		clone.Body = append([]byte(nil), msg.Body...)
	}
	if msg.Headers != nil {
		clone.Headers = cloneHeaders(msg.Headers)
	}
	return clone
}

func cloneHeaders(headers map[string]any) map[string]any {
	clone := make(map[string]any, len(headers))
	for key, value := range headers {
		switch v := value.(type) {
		case []byte:
			clone[key] = append([]byte(nil), v...)
		case map[string]any:
			clone[key] = cloneHeaders(v)
		case []any:
			clone[key] = cloneAnySlice(v)
		default:
			clone[key] = v
		}
	}
	return clone
}

func cloneAnySlice(values []any) []any {
	clone := make([]any, 0, len(values))
	for _, value := range values {
		switch v := value.(type) {
		case []byte:
			clone = append(clone, append([]byte(nil), v...))
		case map[string]any:
			clone = append(clone, cloneHeaders(v))
		case []any:
			clone = append(clone, cloneAnySlice(v))
		default:
			clone = append(clone, v)
		}
	}
	return clone
}
