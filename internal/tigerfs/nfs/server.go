package nfs

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/timescale/tigerfs/internal/tigerfs/config"
	"github.com/timescale/tigerfs/internal/tigerfs/db"
	"github.com/timescale/tigerfs/internal/tigerfs/logging"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
	"go.uber.org/zap"
)

// Server represents an NFS server instance for TigerFS.
type Server struct {
	cfg        *config.Config
	db         *db.Client
	fs         *Filesystem
	listener   net.Listener
	port       int
	mountpoint string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer creates a new NFS server.
func NewServer(ctx context.Context, cfg *config.Config, dbClient *db.Client) (*Server, error) {
	ctx, cancel := context.WithCancel(ctx)

	fs := NewFilesystem(ctx, cfg, dbClient)

	return &Server{
		cfg:    cfg,
		db:     dbClient,
		fs:     fs,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start starts the NFS server on an available port.
func (s *Server) Start() (int, error) {
	// Listen on localhost only for security
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("failed to start NFS listener: %w", err)
	}

	s.listener = listener
	s.port = listener.Addr().(*net.TCPAddr).Port

	logging.Info("NFS server listening",
		zap.Int("port", s.port),
		zap.String("address", listener.Addr().String()))

	// Create NFS handler with caching
	handler := nfshelper.NewNullAuthHandler(s.fs)
	cacheHandler := nfshelper.NewCachingHandler(handler, 1024)

	// Start serving in background
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		logging.Debug("Starting NFS server goroutine")
		if err := nfs.Serve(s.listener, cacheHandler); err != nil {
			// Check if this was a clean shutdown
			select {
			case <-s.ctx.Done():
				logging.Debug("NFS server stopped (context cancelled)")
			default:
				logging.Error("NFS server error", zap.Error(err))
			}
		}
	}()

	return s.port, nil
}

// Port returns the port the server is listening on.
func (s *Server) Port() int {
	return s.port
}

// Stop stops the NFS server.
func (s *Server) Stop() error {
	logging.Debug("Stopping NFS server")

	// Cancel context first
	s.cancel()

	// Close listener to stop accepting new connections
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			logging.Warn("Error closing NFS listener", zap.Error(err))
		}
	}

	// Wait for server goroutine to finish
	s.wg.Wait()

	// Close filesystem
	if s.fs != nil {
		if err := s.fs.Close(); err != nil {
			logging.Warn("Error closing filesystem", zap.Error(err))
		}
	}

	logging.Info("NFS server stopped")
	return nil
}
