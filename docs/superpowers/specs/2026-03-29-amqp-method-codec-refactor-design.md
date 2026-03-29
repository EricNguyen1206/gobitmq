# AMQP Method Codec Refactor Design

## Problem

`internal/amqp/methods.go` is 1037 lines implementing ~25 AMQP 0-9-1 method types. It has three sources of duplication:

1. **Boilerplate interface methods** — `ClassID()` and `MethodID()` are 2-line stubs repeated for every type (~50 lines total).
2. **Nested switch dispatch** — `DecodeMethodFrame` uses an 80-line nested switch over class ID then method ID.
3. **Identical wire layouts** — `ConnectionClose`/`ChannelClose` and `ConnectionTune`/`ConnectionTuneOk` share the same binary format but have separate marshal/decode implementations.

## Goals

- Reduce code without sacrificing type safety or performance.
- Make adding new AMQP methods easier (one map entry + embed header).
- Keep zero reflection, zero code generation, zero new dependencies.

## Design

### 1. Embedded `methodHeader` struct

Replace per-type `ClassID()`/`MethodID()` methods with a single embedded struct:

```go
type methodHeader struct {
    class  uint16
    method uint16
}

func (h methodHeader) ClassID() uint16  { return h.class }
func (h methodHeader) MethodID() uint16 { return h.method }
```

Every method type embeds `methodHeader` as its first field. Example:

```go
type ConnectionStart struct {
    methodHeader
    VersionMajor     byte
    VersionMinor     byte
    ServerProperties Table
    Mechanisms       string
    Locales          string
}
```

The `Method` interface remains unchanged (`ClassID()`, `MethodID()`, `marshal(*bytes.Buffer) error`). The embedding satisfies the interface automatically.

All constructor sites that build method values for encoding must include the `methodHeader` field. For example:

```go
c.sendMethod(0, ConnectionStart{
    methodHeader: methodHeader{classConnection, 10},
    VersionMajor: 0,
    VersionMinor: 9,
    // ...
})
```

### 2. Map-based decode dispatch

Replace the nested switch in `DecodeMethodFrame` with a flat map:

```go
type methodKey uint32

func mkKey(classID, methodID uint16) methodKey {
    return methodKey(uint32(classID)<<16 | uint32(methodID))
}

var methodDecoders = map[methodKey]func(*bytes.Reader) (Method, error){
    mkKey(classConnection, 10): decodeConnectionStart,
    mkKey(classConnection, 11): decodeConnectionStartOk,
    mkKey(classConnection, 30): decodeConnectionTune,
    mkKey(classConnection, 31): decodeConnectionTuneOk,
    mkKey(classConnection, 40): decodeConnectionOpen,
    mkKey(classConnection, 41): decodeConnectionOpenOk,
    mkKey(classConnection, 50): decodeConnectionClose,
    mkKey(classConnection, 51): decodeEmpty[ConnectionCloseOk](methodHeader{classConnection, 51}),
    mkKey(classChannel, 10):    decodeChannelOpen,
    mkKey(classChannel, 11):    decodeChannelOpenOk,
    mkKey(classChannel, 40):    decodeChannelClose,
    mkKey(classChannel, 41):    decodeEmpty[ChannelCloseOk](methodHeader{classChannel, 41}),
    mkKey(classExchange, 10):   decodeExchangeDeclare,
    mkKey(classExchange, 11):   decodeEmpty[ExchangeDeclareOk](methodHeader{classExchange, 11}),
    mkKey(classQueue, 10):      decodeQueueDeclare,
    mkKey(classQueue, 11):      decodeQueueDeclareOk,
    mkKey(classQueue, 20):      decodeQueueBind,
    mkKey(classQueue, 21):      decodeEmpty[QueueBindOk](methodHeader{classQueue, 21}),
    mkKey(classBasic, 10):      decodeBasicQos,
    mkKey(classBasic, 11):      decodeEmpty[BasicQosOk](methodHeader{classBasic, 11}),
    mkKey(classBasic, 20):      decodeBasicConsume,
    mkKey(classBasic, 21):      decodeBasicConsumeOk,
    mkKey(classBasic, 30):      decodeBasicCancel,
    mkKey(classBasic, 31):      decodeBasicCancelOk,
    mkKey(classBasic, 40):      decodeBasicPublish,
    mkKey(classBasic, 60):      decodeBasicDeliver,
    mkKey(classBasic, 80):      decodeBasicAck,
    mkKey(classBasic, 90):      decodeBasicReject,
    mkKey(classBasic, 120):     decodeBasicNack,
    mkKey(classConfirm, 10):    decodeConfirmSelect,
    mkKey(classConfirm, 11):    decodeEmpty[ConfirmSelectOk](methodHeader{classConfirm, 11}),
}
```

For empty `Ok` types that carry no payload, a generic helper avoids writing individual decoder functions:

```go
func decodeEmpty[T any](h methodHeader) func(*bytes.Reader) (Method, error) {
    return func(*bytes.Reader) (Method, error) {
        var zero T
        // Set methodHeader via reflection on the embedded field, or
        // simply return zero value (since ClassID/MethodID come from embedding).
        return zero, nil
    }
}
```

Alternatively, for simplicity and zero reflection, each empty Ok decoder is a one-liner closure at map init time:

```go
mkKey(classConnection, 51): func(*bytes.Reader) (Method, error) {
    return ConnectionCloseOk{methodHeader: methodHeader{classConnection, 51}}, nil
},
```

The updated `DecodeMethodFrame`:

```go
func DecodeMethodFrame(frame Frame) (Method, error) {
    if frame.Type != FrameMethod {
        return nil, fmt.Errorf("amqp: expected method frame, got type %d", frame.Type)
    }
    r := bytes.NewReader(frame.Payload)
    var classID, methodID uint16
    if err := binary.Read(r, binary.BigEndian, &classID); err != nil {
        return nil, err
    }
    if err := binary.Read(r, binary.BigEndian, &methodID); err != nil {
        return nil, err
    }
    dec, ok := methodDecoders[mkKey(classID, methodID)]
    if !ok {
        return nil, fmt.Errorf("amqp: unsupported method %d.%d", classID, methodID)
    }
    return dec(r)
}
```

The map is read-only after initialization. No synchronization is needed.

### 3. Shared codec helpers

#### Close wire format (ConnectionClose / ChannelClose)

Both types encode: `uint16 replyCode`, `shortstr replyText`, `uint16 classRef`, `uint16 methodRef`.

```go
func marshalClose(w *bytes.Buffer, replyCode uint16, replyText string, classRef, methodRef uint16) error {
    binary.Write(w, binary.BigEndian, replyCode) //nolint:errcheck
    if err := writeShortstr(w, replyText); err != nil {
        return err
    }
    binary.Write(w, binary.BigEndian, classRef)  //nolint:errcheck
    binary.Write(w, binary.BigEndian, methodRef) //nolint:errcheck
    return nil
}

func decodeClose(r *bytes.Reader) (replyCode uint16, replyText string, classRef, methodRef uint16, err error) {
    if err = binary.Read(r, binary.BigEndian, &replyCode); err != nil {
        return
    }
    if replyText, err = readShortstr(r); err != nil {
        return
    }
    if err = binary.Read(r, binary.BigEndian, &classRef); err != nil {
        return
    }
    err = binary.Read(r, binary.BigEndian, &methodRef)
    return
}
```

Per-type wrappers:

```go
func (m ConnectionClose) marshal(w *bytes.Buffer) error {
    return marshalClose(w, m.ReplyCode, m.ReplyText, m.ClassIDRef, m.MethodIDRef)
}

func decodeConnectionClose(r *bytes.Reader) (Method, error) {
    rc, rt, cr, mr, err := decodeClose(r)
    if err != nil {
        return nil, err
    }
    return ConnectionClose{methodHeader: methodHeader{classConnection, 50}, ReplyCode: rc, ReplyText: rt, ClassIDRef: cr, MethodIDRef: mr}, nil
}
```

`ChannelClose` follows the same pattern with `methodHeader{classChannel, 40}`.

#### Tune wire format (ConnectionTune / ConnectionTuneOk)

Both types encode: `uint16 channelMax`, `uint32 frameMax`, `uint16 heartbeat`.

```go
func marshalTune(w *bytes.Buffer, channelMax uint16, frameMax uint32, heartbeat uint16) error {
    binary.Write(w, binary.BigEndian, channelMax) //nolint:errcheck
    binary.Write(w, binary.BigEndian, frameMax)   //nolint:errcheck
    binary.Write(w, binary.BigEndian, heartbeat)  //nolint:errcheck
    return nil
}

func decodeTune(r *bytes.Reader) (channelMax uint16, frameMax uint32, heartbeat uint16, err error) {
    if err = binary.Read(r, binary.BigEndian, &channelMax); err != nil {
        return
    }
    if err = binary.Read(r, binary.BigEndian, &frameMax); err != nil {
        return
    }
    err = binary.Read(r, binary.BigEndian, &heartbeat)
    return
}
```

Per-type wrappers delegate to these helpers, wrapping results with the correct `methodHeader`.

### 4. File organization

All changes stay in `internal/amqp/methods.go`. The file remains the single source for method type definitions, marshal/decode logic, and dispatch. No new files are created.

## What changes

| Component | Before | After |
|---|---|---|
| `ClassID()` / `MethodID()` | 50 lines across 25 types | Satisfied by `methodHeader` embedding |
| `DecodeMethodFrame` dispatch | 80-line nested switch | Map lookup + single error path |
| `ConnectionClose` / `ChannelClose` codec | 2 separate implementations | Shared `marshalClose` / `decodeClose` |
| `ConnectionTune` / `ConnectionTuneOk` codec | 2 separate implementations | Shared `marshalTune` / `decodeTune` |
| Empty Ok decoders | Inline in switch | One-liner closures in map |

## What stays the same

- The `Method` interface signature.
- `EncodeMethodFrame` — unchanged.
- All unique per-method `marshal` and `decode` logic.
- Frame encoding/decoding in `frame.go`.
- All other files in `internal/amqp/` and across the project.

## Test updates

- **`TestMethodFrames_RoundTrip`**: Each test case method value must include `methodHeader{...}`. The `reflect.DeepEqual` check works as before since `methodHeader` is a concrete embedded field.
- **New test: `TestDecodeMethodFrame_UnsupportedMethod`**: Verifies that an unknown class/method key returns `"amqp: unsupported method"` error.
- **New test: `TestDecodeClose_RoundTrip`**: Directly tests `marshalClose`/`decodeClose` helpers with `ConnectionClose` and `ChannelClose` values.
- **New test: `TestDecodeTune_RoundTrip`**: Directly tests `marshalTune`/`decodeTune` helpers with `ConnectionTune` and `ConnectionTuneOk` values.
- **New test: `TestMethodDecoders_AllMethodsRegistered`**: Iterates the map and verifies every entry is non-nil.

## Estimated impact

- Lines: ~1037 → ~830
- New test coverage for dispatch and shared helpers
- Zero performance regression (map lookup vs switch is comparable for 25 sparse keys)
- Zero new dependencies

## Edge cases

- **Unknown class/method**: Single error path via map miss — identical behavior to current switch default.
- **Empty payload Ok types**: Decoder closures return zero-value structs with `methodHeader` set. No reader bytes consumed.
- **Partial reads**: Shared helpers (`decodeClose`, `decodeTune`) propagate errors from individual `binary.Read` calls — same error behavior as before.
- **Concurrent map access**: Map is read-only after init. No synchronization needed.
