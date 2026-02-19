package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"mosaic-terminal/internal/config"
	"mosaic-terminal/internal/gateway"
	"mosaic-terminal/internal/router"
	"mosaic-terminal/internal/server"
)

const envGatewayStorePath = "GATEWAY_METADATA_PATH"

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	runtime, err := server.New(cfg, router.DefaultChain())
	if err != nil {
		log.Fatalf("build ssh server: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	componentCount := 1

	go func() {
		errCh <- runtime.Run(ctx)
	}()

	if gatewayServer, ok, err := newGatewayServer(cfg.ListenAddr); err != nil {
		log.Fatalf("build gateway server: %v", err)
	} else if ok {
		componentCount++
		go func() {
			<-ctx.Done()
			_ = gatewayServer.Shutdown(context.Background())
		}()
		go func() {
			log.Printf("level=info event=gateway_startup listen=%s", cfg.ListenAddr)
			errCh <- gatewayServer.ListenAndServe()
		}()
	}

	for i := 0; i < componentCount; i++ {
		err := <-errCh
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed) {
			continue
		}
		log.Fatalf("run service: %v", err)
	}
}

func newGatewayServer(listenAddr string) (*http.Server, bool, error) {
	if os.Getenv("GATEWAY_HMAC_SECRET") == "" {
		log.Printf("level=info event=gateway_disabled reason=missing_hmac_secret")
		return nil, false, nil
	}

	svc, err := gateway.NewService(gateway.NewSSHLauncher(), gateway.NewFileMetadataStore(os.Getenv(envGatewayStorePath)))
	if err != nil {
		return nil, false, err
	}

	return &http.Server{Addr: listenAddr, Handler: gateway.NewHandler(svc).Routes()}, true, nil
}
