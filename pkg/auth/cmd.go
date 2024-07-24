package auth

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	config2 "github.com/kaytu-io/kaytu-util/pkg/config"
	"github.com/kaytu-io/kaytu-util/pkg/httpserver"
	"github.com/kaytu-io/kaytu-util/pkg/postgres"
	"net"
	"os"
	"strconv"

	"github.com/kaytu-io/kaytu-engine/pkg/auth/auth0"
	"github.com/kaytu-io/kaytu-engine/pkg/auth/db"

	"github.com/kaytu-io/kaytu-engine/pkg/workspace/client"

	envoyauth "github.com/envoyproxy/go-control-plane/envoy/service/auth/v3"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	mailApiKey     = os.Getenv("EMAIL_API_KEY")
	mailSender     = os.Getenv("EMAIL_SENDER")
	mailSenderName = os.Getenv("EMAIL_SENDER_NAME")

	auth0Domain                  = os.Getenv("AUTH0_DOMAIN")
	auth0ClientID                = os.Getenv("AUTH0_CLIENT_ID")
	auth0ClientIDNative          = os.Getenv("AUTH0_CLIENT_ID_NATIVE")
	auth0ClientIDPennywiseNative = os.Getenv("AUTH0_CLIENT_ID_PENNYWISE_NATIVE")

	auth0ManageDomain       = os.Getenv("AUTH0_MANAGE_DOMAIN")
	auth0ManageClientID     = os.Getenv("AUTH0_MANAGE_CLIENT_ID")
	auth0ManageClientSecret = os.Getenv("AUTH0_MANAGE_CLIENT_SECRET")
	auth0Connection         = os.Getenv("AUTH0_CONNECTION")
	auth0InviteTTL          = os.Getenv("AUTH0_INVITE_TTL")

	httpServerAddress = os.Getenv("HTTP_ADDRESS")

	kaytuHost       = os.Getenv("KAYTU_HOST")
	kaytuPublicKey  = os.Getenv("KAYTU_PUBLIC_KEY")
	kaytuPrivateKey = os.Getenv("KAYTU_PRIVATE_KEY")

	workspaceBaseUrl = os.Getenv("WORKSPACE_BASE_URL")
	metadataBaseUrl  = os.Getenv("METADATA_BASE_URL")

	grpcServerAddress = os.Getenv("GRPC_ADDRESS")
	grpcTlsCertPath   = os.Getenv("GRPC_TLS_CERT_PATH")
	grpcTlsKeyPath    = os.Getenv("GRPC_TLS_KEY_PATH")
	grpcTlsCAPath     = os.Getenv("GRPC_TLS_CA_PATH")
)

func Command() *cobra.Command {
	return &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			return start(cmd.Context())
		},
	}
}

type ServerConfig struct {
	PostgreSQL config2.Postgres
}

// start runs both HTTP and GRPC server.
// GRPC server has Check method to ensure user is
// authenticated and authorized to perform an action.
// HTTP server has multiple endpoints to view and update
// the user roles.
func start(ctx context.Context) error {
	var conf ServerConfig
	config2.ReadFromEnv(&conf, nil)

	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}

	verifier, err := newAuth0OidcVerifier(ctx, auth0Domain, auth0ClientID)
	if err != nil {
		return fmt.Errorf("open id connect verifier: %w", err)
	}

	verifierNative, err := newAuth0OidcVerifier(ctx, auth0Domain, auth0ClientIDNative)
	if err != nil {
		return fmt.Errorf("open id connect verifier: %w", err)
	}

	verifierPennywiseNative, err := newAuth0OidcVerifier(ctx, auth0Domain, auth0ClientIDPennywiseNative)
	if err != nil {
		return fmt.Errorf("open id connect verifier pennywise: %w", err)
	}

	logger.Info("Instantiated a new Open ID Connect verifier")
	//m := email.NewSendGridClient(mailApiKey, mailSender, mailSenderName, logger)

	creds, err := newServerCredentials(
		grpcTlsCertPath,
		grpcTlsKeyPath,
		grpcTlsCAPath,
	)
	if err != nil {
		return fmt.Errorf("grpc tls creds: %w", err)
	}

	workspaceClient := client.NewWorkspaceClient(workspaceBaseUrl)

	b, err := base64.StdEncoding.DecodeString(kaytuPublicKey)
	if err != nil {
		return fmt.Errorf("public key decode: %w", err)
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return fmt.Errorf("failed to decode my private key")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return err
	}

	b, err = base64.StdEncoding.DecodeString(kaytuPrivateKey)
	if err != nil {
		return fmt.Errorf("public key decode: %w", err)
	}
	block, _ = pem.Decode(b)
	if block == nil {
		panic("failed to decode private key")
	}
	pri, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		panic(err)
	}

	inviteTTL, err := strconv.ParseInt(auth0InviteTTL, 10, 64)
	if err != nil {
		return err
	}

	// setup postgres connection
	cfg := postgres.Config{
		Host:    conf.PostgreSQL.Host,
		Port:    conf.PostgreSQL.Port,
		User:    conf.PostgreSQL.Username,
		Passwd:  conf.PostgreSQL.Password,
		DB:      conf.PostgreSQL.DB,
		SSLMode: conf.PostgreSQL.SSLMode,
	}
	orm, err := postgres.NewClient(&cfg, logger)
	if err != nil {
		return fmt.Errorf("new postgres client: %w", err)
	}

	adb := db.Database{Orm: orm}
	fmt.Println("Connected to the postgres database: ", conf.PostgreSQL.DB)

	err = adb.Initialize()
	if err != nil {
		return fmt.Errorf("new postgres client: %w", err)
	}

	auth0Service := auth0.New(auth0ManageDomain, auth0ClientID, auth0ManageClientID, auth0ManageClientSecret,
		auth0Connection, int(inviteTTL))

	authServer := &Server{
		host:                    kaytuHost,
		kaytuPublicKey:          pub.(*rsa.PublicKey),
		verifier:                verifier,
		verifierNative:          verifierNative,
		verifierPennywiseNative: verifierPennywiseNative,
		logger:                  logger,
		workspaceClient:         workspaceClient,
		db:                      adb,
		auth0Service:            auth0Service,
		updateLoginUserList:     nil,
		updateLogin:             make(chan User, 100000),
	}
	go authServer.WorkspaceMapUpdater()
	go authServer.UpdateLastLoginLoop()

	grpcServer := grpc.NewServer(grpc.Creds(creds))
	envoyauth.RegisterAuthorizationServer(grpcServer, authServer)

	lis, err := net.Listen("tcp", grpcServerAddress)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	errors := make(chan error, 1)
	go func() {
		errors <- fmt.Errorf("grpc server: %w", grpcServer.Serve(lis))
	}()

	go func() {
		routes := httpRoutes{
			logger: logger,
			//emailService:    m,
			workspaceClient: workspaceClient,
			metadataBaseUrl: metadataBaseUrl,
			auth0Service:    auth0Service,
			kaytuPrivateKey: pri.(*rsa.PrivateKey),
			db:              adb,
			authServer:      authServer,
		}
		errors <- fmt.Errorf("http server: %w", httpserver.RegisterAndStart(ctx, logger, httpServerAddress, &routes))
	}()

	return <-errors
}

// newServerCredentials loads TLS transport credentials for the GRPC server.
func newServerCredentials(certPath string, keyPath string, caPath string) (credentials.TransportCredentials, error) {
	srv, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}

	p := x509.NewCertPool()

	if caPath != "" {
		ca, err := os.ReadFile(caPath)
		if err != nil {
			return nil, err
		}

		p.AppendCertsFromPEM(ca)
	}

	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{srv},
		RootCAs:      p,
	}), nil
}
