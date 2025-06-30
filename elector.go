package elector

import (
	"context"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	redis "github.com/redis/rueidis"
)

var (
	// ErrNonLeader - the instance is not the leader
	ErrNonLeader = errors.New("the elector is not leader")
	// ErrClosed - the elector is already closed
	ErrClosed = errors.New("the elector is already closed")
	// ErrPingRedis - ping redis server timeout
	ErrPingRedis = errors.New("ping redis server timeout")
)

type (
	// Config is an alias redis.ClientOption
	Config = redis.ClientOption
)

const (
	script = `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("pexpire", KEYS[1], ARGV[2])
	else
		if redis.call("exists", KEYS[1]) == 0 then
			redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2], "NX")
			return 1
		end
		
		-- if the key exists but not owned by this instance, return 0
		return 0
	end
	`
)

// Elector is a distributed leader election implementation using redis.
type Elector struct {
	ctx    context.Context
	cancel context.CancelFunc

	config        redis.ClientOption
	client        redis.Client
	id            string        // unique ID for the elector, internal use only!
	ttl           time.Duration // TTL for the elector
	renewInterval time.Duration // renew interval for the elector
	key           string        // key for the elector, default is "/gocron/elector/"

	mu       sync.RWMutex
	closed   bool
	isLeader bool
	leaderID string

	logger *zap.SugaredLogger
}

type Option func(*Elector)

// WithTTL sets the TTL for the elector.
func WithTTL(ttl int) Option {
	return func(e *Elector) {
		e.ttl = time.Duration(ttl) * time.Second

		jitter := time.Duration(mrand.Intn(500)) * time.Millisecond
		e.renewInterval = e.ttl/3 + jitter // renew interval is 1/3 of TTL with some jitter
	}
}

// WithKey sets the key for the elector.
func WithKey(key string) Option {
	return func(e *Elector) {
		e.key = key
	}
}

// WithLogger sets the logger for the elector.
func WithLogger(logger *zap.SugaredLogger) Option {
	return func(e *Elector) {
		e.logger = logger.Named("ELECTOR_CRON")
	}
}

// NewElector creates a new Elector instance with the given redis config.
func NewElector(ctx context.Context, cfg redis.ClientOption, opts ...Option) (*Elector, error) {
	return newElector(ctx, nil, cfg, opts...)
}

// NewElectorWithClient creates a new Elector instance with the given etcd client.
func NewElectorWithClient(ctx context.Context, cli redis.Client, opts ...Option) (*Elector, error) {
	return newElector(ctx, cli, Config{}, opts...)
}

func newElector(ctx context.Context, cli redis.Client, cfg redis.ClientOption, opts ...Option) (*Elector, error) {
	var err error
	if cli == nil {
		cli, err = redis.NewClient(cfg) // async dial redis
		if err != nil {
			return nil, err
		}
	}

	cctx, cancel := context.WithCancel(ctx)
	el := &Elector{
		ctx:    cctx,
		cancel: cancel,
		config: cfg,
		id:     getID(),
		client: cli,
	}

	for _, opt := range opts {
		opt(el)
	}

	// set defauls
	if el.key == "" {
		WithKey("elector:cron")(el) // default key is "elector:cron"
	}

	if el.ttl == 0 {
		WithTTL(10)(el) // default TTL is 10 seconds
	}

	if el.logger == nil {
		nopLogger := *zap.NewNop().Sugar()
		WithLogger(&nopLogger)(el) // use a no-op logger if not provided
	}

	err = el.ping()
	if err != nil {
		return nil, err
	}

	return el, nil
}

// GetID returns the elector ID.
func (e *Elector) GetID() string {
	return e.id
}

// GetLeaderID returns the current leader ID.
func (e *Elector) GetLeaderID() string {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.leaderID
}

// Stop stops the elector and closes the etcd client.
func (e *Elector) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	e.cancel()
	e.closed = true
	e.client.Close()

	return nil
}

// IsLeader checks if the current instance is the leader.
func (e *Elector) IsLeader(_ context.Context) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.isLeader {
		return nil
	}

	return ErrNonLeader
}

func (e *Elector) setLeader(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.isLeader = true
	e.leaderID = id
}

func (e *Elector) unsetLeader(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.isLeader = false
	e.leaderID = id
}

func (e *Elector) ping() error {
	timeoutCtx, cancel := context.WithTimeout(e.ctx, 6*time.Second)
	defer cancel()

	_ = e.client.Do(timeoutCtx, e.client.B().Ping().Build())

	if timeoutCtx.Err() == context.DeadlineExceeded {
		return ErrPingRedis
	}
	return nil
}

// Start Start the election.
// This method will keep trying the election. When the election is successful, set isleader to true.
// If it fails, the election directory will be monitored until the election is successful.
// The parameter electionPath is used to specify the etcd directory for the operation.
func (e *Elector) Start() error {
	e.logger.Infow("Starting elector", "id", e.id, "key", e.key, "ttl", e.ttl, "renewInterval", e.renewInterval)
	if e.closed {
		return ErrClosed
	}

	cmd := e.client.B().Set().Key(e.key).Value(e.id).Nx().Ex(e.ttl).Build()
	resp := e.client.Do(e.ctx, cmd)

	if err := resp.Error(); err != nil {
		e.logger.Warnw("Another instance is likely the leader", "error", err)
	}

	defer func() {
		// unset leader
		e.unsetLeader("")
	}()

	ticker := time.NewTicker(e.renewInterval)
	defer ticker.Stop()

	for e.ctx.Err() == nil {
		select {
		case <-ticker.C:
			cmd := e.client.B().Eval().Script(script).
				Numkeys(1).
				Key(e.key).
				Arg(e.id).
				Arg(fmt.Sprintf("%d", e.ttl.Milliseconds())).Build()

			res := e.client.Do(e.ctx, cmd)
			if err := res.Error(); err != nil {
				e.logger.Errorw("Error renewing lock:", "error", err, "id", e.id)
			} else {
				n, _ := res.AsInt64()
				switch n {
				case 1:
					e.logger.Debugw("Lock renewed.", "id", e.id)
					if e.IsLeader(e.ctx) != nil { // is non-leader
						e.setLeader(e.id)
						e.logger.Debugw("switch to leader, the current instance is leader", "id", e.id)
					}
				case 0:
					e.logger.Warnw("Failed to renew lock, another instance may be the leader", "id", e.id)
				}
			}
		case <-e.ctx.Done():
			return nil
		}
	}

	return nil
}

func getID() string {
	hostname, _ := os.Hostname()
	bs := make([]byte, 10)
	_, err := rand.Read(bs)
	if err != nil {
		return fmt.Sprintf("%s-%d", hostname, mrand.Int63())
	}
	return fmt.Sprintf("%s-%x", hostname, bs)
}
