package grpc_server

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/alitto/pond"
	"github.com/google/uuid"
	"github.com/kaytu-io/kaytu-engine/pkg/utils"
	"github.com/kaytu-io/kaytu-engine/services/wastage/api/wastage/limit"
	"github.com/kaytu-io/kaytu-engine/services/wastage/config"
	"github.com/kaytu-io/kaytu-engine/services/wastage/db/repo"
	"github.com/kaytu-io/kaytu-engine/services/wastage/recommendation"
	awsPluginProto "github.com/kaytu-io/plugin-aws/plugin/proto/src/golang"
	gcpPluginProto "github.com/kaytu-io/plugin-gcp/plugin/proto/src/golang/gcp"
	kubernetesPluginProto "github.com/kaytu-io/plugin-kubernetes-internal/plugin/proto/src/golang"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"net"
	"time"
)

type Server struct {
	logger                 *zap.Logger
	kubernetesPluginServer *kubernetesPluginServer
	gcpPluginServer        *gcpPluginServer
	awsPluginServer        *awsPluginServer
}

func NewServer(logger *zap.Logger, cfg config.WastageConfig, blobClient *azblob.Client, blobWorkerPool *pond.WorkerPool,
	usageRepo repo.UsageV2Repo, recomSvc *recommendation.Service, limitSvc *limit.Service) *Server {
	kuberServer := newKubernetesPluginServer(logger, cfg, blobClient, blobWorkerPool, usageRepo, recomSvc)
	gcpServer := newGcpPluginServer(logger, cfg, blobClient, blobWorkerPool, usageRepo, recomSvc)
	awsServer := newAwsPluginServer(logger, cfg, blobClient, blobWorkerPool, usageRepo, recomSvc, limitSvc)

	svr := Server{
		logger:                 logger,
		kubernetesPluginServer: kuberServer,
		gcpPluginServer:        gcpServer,
		awsPluginServer:        awsServer,
	}
	return &svr
}

func Logger(logger *zap.Logger) func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		reqId := uuid.New()

		logger.Info("Request", zap.String("ReqID", reqId.String()))
		startTime := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(startTime).Seconds()
		if err != nil {
			logger.Error("Request failed", zap.String("ReqID", reqId.String()), zap.Error(err), zap.Float64("latency", elapsed))
		} else {
			logger.Info("Request succeeded", zap.String("ReqID", reqId.String()), zap.Float64("latency", elapsed))
		}

		return resp, err
	}
}

func StartGrpcServer(server *Server, grpcServerAddress string, authGRPCURI string) error {
	lis, err := net.Listen("tcp", grpcServerAddress)
	if err != nil {
		server.logger.Error("failed to listen", zap.Error(err))
		return err
	}

	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(256*1024*1024),
		grpc.ChainUnaryInterceptor(Logger(server.logger)),
		grpc.ConnectionTimeout(10*time.Minute),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 10 * time.Minute,
			Time:              10 * time.Minute,
			Timeout:           10 * time.Minute,
		}),
	)
	kubernetesPluginProto.RegisterOptimizationServer(s, server.kubernetesPluginServer)
	gcpPluginProto.RegisterOptimizationServer(s, server.gcpPluginServer)
	awsPluginProto.RegisterOptimizationServer(s, server.awsPluginServer)
	server.logger.Info("server listening at", zap.String("address", lis.Addr().String()))
	utils.EnsureRunGoroutine(func() {
		if err = s.Serve(lis); err != nil {
			server.logger.Error("failed to serve", zap.Error(err))
			panic(err)
		}
	})
	return nil
}
