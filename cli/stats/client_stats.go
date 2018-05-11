package stats

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"context"
	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/openconfig/gnmi/client"
	"github.com/openconfig/gnmi/value"
	"github.com/openconfig/ygot/ygot"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	gpb "github.com/openconfig/gnmi/proto/gnmi"
)

// Type defines the name resolution for this client type.
const Type = "gnmi"

type Record struct {
	Rts []time.Time //request time stamp
	Sts []time.Time //sync time stamp
}

// Client handles execution of the query and caching of its results.
type Client struct {
	conn      *grpc.ClientConn
	client    gpb.GNMIClient
	sub       gpb.GNMI_SubscribeClient
	query     client.Query
	recv      client.ProtoHandler
	handler   client.NotificationHandler
	connected bool
	Rd        Record
}

// New returns a new initialized client. If error is nil, returned Client has
// established a connection to d. Close needs to be called for cleanup.
func NewStatsClient(ctx context.Context, d client.Destination) (*Client, error) {
	if len(d.Addrs) != 1 {
		return nil, fmt.Errorf("d.Addrs must only contain one entry: %v", d.Addrs)
	}
	opts := []grpc.DialOption{
		grpc.WithTimeout(d.Timeout),
		grpc.WithBlock(),
	}
	if d.TLS != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(d.TLS)))
	}
	conn, err := grpc.DialContext(ctx, d.Addrs[0], opts...)
	if err != nil {
		return nil, fmt.Errorf("Dialer(%s, %v): %v", d.Addrs[0], d.Timeout, err)
	}
	return NewFromConn(ctx, conn, d)
}

// NewFromConn creates and returns the client based on the provided transport.
func NewFromConn(ctx context.Context, conn *grpc.ClientConn, d client.Destination) (*Client, error) {
	cl := gpb.NewGNMIClient(conn)
	return &Client{
		conn:   conn,
		client: cl,
	}, nil
}

// Subscribe sends the gNMI Subscribe RPC to the server.
func (c *Client) Subscribe(ctx context.Context, q client.Query) error {
	sub, err := c.client.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("gpb.GNMIClient.Subscribe(%v) failed to initialize Subscribe RPC: %v", q, err)
	}
	qq, err := subscribe(q)
	if err != nil {
		return fmt.Errorf("generating SubscribeRequest proto: %v", err)
	}
	if err := sub.Send(qq); err != nil {
		return fmt.Errorf("client.Send(%+v): %v", qq, err)
	}

	c.sub = sub
	c.query = q

	c.Rd.Rts = append(c.Rd.Rts, time.Now())
	log.V(3).Infof("Subscribe:  c.Rd %v", c.Rd)
	if q.ProtoHandler == nil {
		c.recv = c.defaultRecv
		c.handler = q.NotificationHandler
	} else {
		c.recv = q.ProtoHandler
	}
	return nil
}

// Poll will send a single gNMI poll request to the server.
func (c *Client) Poll() error {
	if err := c.sub.Send(&gpb.SubscribeRequest{Request: &gpb.SubscribeRequest_Poll{Poll: &gpb.Poll{}}}); err != nil {
		return fmt.Errorf("client.Poll(): %v", err)
	}
	c.Rd.Rts = append(c.Rd.Rts, time.Now())
	log.V(3).Infof("Poll:  c.Rd %v", c.Rd)
	return nil
}

// Peer returns the peer of the current stream. If the client is not created or
// if the peer is not valid nil is returned.
func (c *Client) Peer() string {
	return c.query.Addrs[0]
}

// Close forcefully closes the underlying connection, terminating the query
// right away. It's safe to call Close multiple times.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Recv will recieve a single message from the server and process it based on
// the provided handlers (Proto or Notification).
func (c *Client) Recv() error {
	n, err := c.sub.Recv()
	if err != nil {
		return err
	}
	log.V(3).Infof("Recv:  n %v", n)
	return c.recv(n)
}

func (c *Client) RecvAll() error {
	for {
		err := c.Recv()
		switch err {
		default:
			log.V(1).Infof("RecvAll received unknown error: %v", err)
			c.Close()
			return err
		case io.EOF, client.ErrStopReading:
			log.V(3).Infof("RecvAll stop marker: %v", err)
			return nil
		case nil:
		}

		// Close fast, so that we don't deliver any buffered updates.
		//
		// Note: this approach still allows at most 1 update through after
		// Close. A more thorough solution would be to do the check at
		// Notification/ProtoHandler or Impl level, but that would involve much
		// more work.
		/*
			c.mu.RLock()
			closed := c.closed
			c.mu.RUnlock()
			if closed {
				return nil
			}
		*/
	}
}

// defaultRecv is the default implementation of recv provided by the client.
// This function will be replaced by the ProtoHandler member of the Query
// struct passed to New(), if it is set.
func (c *Client) defaultRecv(msg proto.Message) error {
	if !c.connected {
		c.handler(client.Connected{})
		c.connected = true
	}

	resp, ok := msg.(*gpb.SubscribeResponse)
	if !ok {
		return fmt.Errorf("failed to type assert message %#v", msg)
	}
	log.V(5).Info(resp)
	switch v := resp.Response.(type) {
	default:
		return fmt.Errorf("unknown response %T: %s", v, v)
	case *gpb.SubscribeResponse_Error:
		return fmt.Errorf("error in response: %s", v)
	case *gpb.SubscribeResponse_SyncResponse:
		c.handler(client.Sync{})
		c.Rd.Sts = append(c.Rd.Sts, time.Now())
		log.V(5).Infof("defaultRecv:  c.Rd %v", c.Rd)
		if c.query.Type == client.Poll || c.query.Type == client.Once {
			return client.ErrStopReading
		}
	case *gpb.SubscribeResponse_Update:
		n := v.Update
		var p []string
		if n.Prefix != nil {
			var err error
			p, err = ygot.PathToStrings(n.Prefix)
			if err != nil {
				return err
			}
		}
		ts := time.Unix(0, n.Timestamp)
		for _, u := range n.Update {
			if u.Path == nil {
				return fmt.Errorf("invalid nil path in update: %v", u)
			}
			u, err := noti(p, u.Path, ts, u)
			if err != nil {
				return err
			}
			c.handler(u)
		}
		for _, d := range n.Delete {
			u, err := noti(p, d, ts, nil)
			if err != nil {
				return err
			}
			c.handler(u)
		}
	}
	return nil
}

func getType(t client.Type) gpb.SubscriptionList_Mode {
	switch t {
	case client.Once:
		return gpb.SubscriptionList_ONCE
	case client.Stream:
		return gpb.SubscriptionList_STREAM
	case client.Poll:
		return gpb.SubscriptionList_POLL
	}
	return gpb.SubscriptionList_ONCE
}

func subscribe(q client.Query) (*gpb.SubscribeRequest, error) {
	s := &gpb.SubscribeRequest_Subscribe{
		Subscribe: &gpb.SubscriptionList{
			Mode:   getType(q.Type),
			Prefix: &gpb.Path{Target: q.Target},
		},
	}
	for _, qq := range q.Queries {
		pp, err := ygot.StringToPath(pathToString(qq), ygot.StructuredPath, ygot.StringSlicePath)
		if err != nil {
			return nil, fmt.Errorf("invalid query path %q: %v", qq, err)
		}
		s.Subscribe.Subscription = append(s.Subscribe.Subscription, &gpb.Subscription{Path: pp})
	}
	return &gpb.SubscribeRequest{Request: s}, nil
}

// decimalToFloat converts a *gnmi_proto.Decimal64 to a float32. Downcasting to float32 is performed as the
// precision of a float64 is not required.
func decimalToFloat(d *gpb.Decimal64) float32 {
	return float32(float64(d.Digits) / math.Pow(10, float64(d.Precision)))
}

// ToScalar will convert TypedValue scalar types to a Go native type. It will
// return an error if the TypedValue does not contain a scalar type.
func toScalar(tv *gpb.TypedValue) (interface{}, error) {
	var i interface{}
	switch tv.Value.(type) {
	case *gpb.TypedValue_DecimalVal:
		i = decimalToFloat(tv.GetDecimalVal())
	case *gpb.TypedValue_StringVal:
		i = tv.GetStringVal()
	case *gpb.TypedValue_IntVal:
		i = tv.GetIntVal()
	case *gpb.TypedValue_UintVal:
		i = tv.GetUintVal()
	case *gpb.TypedValue_BoolVal:
		i = tv.GetBoolVal()
	case *gpb.TypedValue_FloatVal:
		i = tv.GetFloatVal()
	case *gpb.TypedValue_LeaflistVal:
		elems := tv.GetLeaflistVal().GetElement()
		ss := make([]interface{}, len(elems))
		for x, e := range elems {
			v, err := toScalar(e)
			if err != nil {
				return nil, fmt.Errorf("ToScalar for ScalarArray %+v: %v", e.Value, err)
			}
			ss[x] = v
		}
		i = ss
	case *gpb.TypedValue_BytesVal:
		i = tv.GetBytesVal()
	case *gpb.TypedValue_JsonIetfVal:
		//var val interface{}
		//val = tv.GetJsonIetfVal()
		//json.Unmarshal(val.([]byte), &i)
		i = tv.GetJsonIetfVal()
	default:
		return nil, fmt.Errorf("non-scalar type %+v", tv.Value)
	}
	return i, nil
}

func noti(prefix []string, pp *gpb.Path, ts time.Time, u *gpb.Update) (client.Notification, error) {
	sp, err := ygot.PathToStrings(pp)
	if err != nil {
		return nil, fmt.Errorf("converting path %v to []string: %v", u.GetPath(), err)
	}
	// Make a full new copy of prefix + u.Path to avoid any reuse of underlying
	// slice arrays.
	p := make([]string, 0, len(prefix)+len(sp))
	p = append(p, prefix...)
	p = append(p, sp...)

	if u == nil {
		return client.Delete{Path: p, TS: ts}, nil
	}
	if u.Val != nil {
		val, err := toScalar(u.Val)
		if err != nil {
			return nil, err
		}
		return client.Update{Path: p, TS: ts, Val: val}, nil
	}
	switch v := u.Value; v.Type {
	case gpb.Encoding_BYTES:
		return client.Update{Path: p, TS: ts, Val: v.Value}, nil
	case gpb.Encoding_JSON, gpb.Encoding_JSON_IETF:
		var val interface{}
		if err := json.Unmarshal(v.Value, &val); err != nil {
			return nil, fmt.Errorf("json.Unmarshal(%q, val): %v", v, err)
		}
		return client.Update{Path: p, TS: ts, Val: val}, nil
	default:
		return nil, fmt.Errorf("Unsupported value type: %v", v.Type)
	}
}

// ProtoResponse converts client library Notification types into gNMI
// SubscribeResponse proto. An error is returned if any notifications have
// invalid paths or if update values can't be converted to gpb.TypedValue.
func ProtoResponse(notifs ...client.Notification) (*gpb.SubscribeResponse, error) {
	n := new(gpb.Notification)

	for _, nn := range notifs {
		switch nn := nn.(type) {
		case client.Update:
			if n.Timestamp == 0 {
				n.Timestamp = nn.TS.UnixNano()
			}

			pp, err := ygot.StringToPath(pathToString(nn.Path), ygot.StructuredPath, ygot.StringSlicePath)
			if err != nil {
				return nil, err
			}
			v, err := value.FromScalar(nn.Val)
			if err != nil {
				return nil, err
			}

			n.Update = append(n.Update, &gpb.Update{
				Path: pp,
				Val:  v,
			})

		case client.Delete:
			if n.Timestamp == 0 {
				n.Timestamp = nn.TS.UnixNano()
			}

			pp, err := ygot.StringToPath(pathToString(nn.Path), ygot.StructuredPath, ygot.StringSlicePath)
			if err != nil {
				return nil, err
			}
			n.Delete = append(n.Delete, pp)

		default:
			return nil, fmt.Errorf("gnmi.ProtoResponse: unsupported type %T", nn)
		}
	}

	resp := &gpb.SubscribeResponse{Response: &gpb.SubscribeResponse_Update{Update: n}}
	return resp, nil
}

func pathToString(q client.Path) string {
	qq := make(client.Path, len(q))
	copy(qq, q)
	// Escape all slashes within a path element. ygot.StringToPath will handle
	// these escapes.
	for i, e := range qq {
		qq[i] = strings.Replace(e, "/", "\\/", -1)
	}
	return strings.Join(qq, "/")
}
