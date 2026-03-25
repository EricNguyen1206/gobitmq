package store

func CloneMessage(msg Message) Message {
	clone := msg
	clone.Body = append([]byte(nil), msg.Body...)
	clone.Headers = CloneTable(msg.Headers)
	return clone
}

func CloneTable(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	clone := make(map[string]any, len(values))
	for key, value := range values {
		clone[key] = cloneValue(value)
	}
	return clone
}

func cloneValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...)
	case map[string]any:
		return CloneTable(v)
	case []any:
		clone := make([]any, len(v))
		for i, item := range v {
			clone[i] = cloneValue(item)
		}
		return clone
	default:
		return v
	}
}
