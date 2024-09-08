package workspace

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	api6 "github.com/hashicorp/vault/api"
	api5 "github.com/kaytu-io/kaytu-engine/pkg/analytics/api"
	api3 "github.com/kaytu-io/kaytu-engine/pkg/describe/api"
	client3 "github.com/kaytu-io/kaytu-engine/pkg/describe/client"
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/config"
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/db"
	db2 "github.com/kaytu-io/kaytu-engine/pkg/workspace/db"
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/statemanager"
	api2 "github.com/kaytu-io/kaytu-util/pkg/api"
	"github.com/kaytu-io/kaytu-util/pkg/httpclient"
	httpserver2 "github.com/kaytu-io/kaytu-util/pkg/httpserver"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"github.com/kaytu-io/kaytu-util/pkg/vault"
	"net/http"
	"strconv"
	"strings"

	"github.com/kaytu-io/kaytu-engine/pkg/onboard/client"

	client2 "github.com/kaytu-io/kaytu-engine/pkg/inventory/client"

	v1 "k8s.io/api/apps/v1"

	"github.com/labstack/gommon/log"

	corev1 "k8s.io/api/core/v1"

	authapi "github.com/kaytu-io/kaytu-engine/pkg/auth/api"
	authclient "github.com/kaytu-io/kaytu-engine/pkg/auth/client"
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/api"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	ErrInternalServer = errors.New("internal server error")
)

type Server struct {
	logger             *zap.Logger
	e                  *echo.Echo
	cfg                config.Config
	db                 *db.Database
	authClient         authclient.AuthServiceClient
	kubeClient         k8sclient.Client // the kubernetes client
	StateManager       *statemanager.Service
	vault              vault.VaultSourceConfig
	vaultSecretHandler vault.VaultSecretHandler
}

func NewServer(ctx context.Context, logger *zap.Logger, cfg config.Config) (*Server, error) {
	s := &Server{
		cfg: cfg,
	}

	s.e, _ = httpserver2.Register(logger, s)

	dbs, err := db.NewDatabase(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("new database: %w", err)
	}
	s.db = dbs

	kubeClient, err := statemanager.NewKubeClient()
	if err != nil {
		return nil, fmt.Errorf("new kube client: %w", err)
	}
	s.kubeClient = kubeClient

	err = v1.AddToScheme(s.kubeClient.Scheme())
	if err != nil {
		return nil, fmt.Errorf("add v1 to scheme: %w", err)
	}

	s.authClient = authclient.NewAuthServiceClient(cfg.Auth.BaseURL)

	s.logger = logger

	switch cfg.Vault.Provider {
	case vault.AwsKMS:
		s.vault, err = vault.NewKMSVaultSourceConfig(ctx, cfg.Vault.Aws, cfg.Vault.KeyId)
		if err != nil {
			logger.Error("new kms vaultClient source config", zap.Error(err))
			return nil, fmt.Errorf("new kms vaultClient source config: %w", err)
		}
	case vault.AzureKeyVault:
		s.vault, err = vault.NewAzureVaultClient(ctx, logger, cfg.Vault.Azure, cfg.Vault.KeyId)
		if err != nil {
			logger.Error("new azure vaultClient source config", zap.Error(err))
			return nil, fmt.Errorf("new azure vaultClient source config: %w", err)
		}
		s.vaultSecretHandler, err = vault.NewAzureVaultSecretHandler(logger, cfg.Vault.Azure)
		if err != nil {
			logger.Error("new azure vaultClient secret handler", zap.Error(err))
			return nil, fmt.Errorf("new azure vaultClient secret handler: %w", err)
		}
	case vault.HashiCorpVault:
		s.vaultSecretHandler, err = vault.NewHashiCorpVaultSecretHandler(ctx, logger, cfg.Vault.HashiCorp)
		if err != nil {
			logger.Error("new hashicorp vaultClient secret handler", zap.Error(err))
			return nil, fmt.Errorf("new hashicorp vaultClient secret handler: %w", err)
		}

		s.vault, err = vault.NewHashiCorpVaultClient(ctx, logger, cfg.Vault.HashiCorp, cfg.Vault.KeyId)
		if err != nil {
			if strings.Contains(err.Error(), api6.ErrSecretNotFound.Error()) {
				b := make([]byte, 32)
				_, err := rand.Read(b)
				if err != nil {
					return nil, err
				}

				_, err = s.vaultSecretHandler.SetSecret(ctx, cfg.Vault.KeyId, b)
				if err != nil {
					return nil, err
				}

				s.vault, err = vault.NewHashiCorpVaultClient(ctx, logger, cfg.Vault.HashiCorp, cfg.Vault.KeyId)
				if err != nil {
					logger.Error("new hashicorp vaultClient source config after setSecret", zap.Error(err))
					return nil, fmt.Errorf("new hashicorp vaultClient source config after setSecret: %w", err)
				}
			} else {
				logger.Error("new hashicorp vaultClient source config", zap.Error(err))
				return nil, fmt.Errorf("new hashicorp vaultClient source config: %w", err)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported vault provider: %s", cfg.Vault.Provider)
	}

	s.StateManager, err = statemanager.New(ctx, cfg, s.vault, s.vaultSecretHandler, s.db, s.kubeClient)
	if err != nil {
		return nil, fmt.Errorf("failed to load initiate state manager: %v", err)
	}

	return s, nil
}

func (s *Server) Register(e *echo.Echo) {
	v1Group := e.Group("/api/v1")

	workspaceGroup := v1Group.Group("/workspace")
	workspaceGroup.GET("/current", httpserver2.AuthorizeHandler(s.GetCurrentWorkspace, api2.ViewerRole))
	workspaceGroup.POST("/:workspace_id/owner", httpserver2.AuthorizeHandler(s.ChangeOwnership, api2.EditorRole))
	workspaceGroup.POST("/:workspace_id/organization", httpserver2.AuthorizeHandler(s.ChangeOrganization, api2.KaytuAdminRole))

	bootstrapGroup := v1Group.Group("/bootstrap")
	bootstrapGroup.GET("/:workspace_name", httpserver2.AuthorizeHandler(s.GetBootstrapStatus, api2.EditorRole))

	workspacesGroup := v1Group.Group("/workspaces")
	workspacesGroup.GET("/limits/:workspace_name", httpserver2.AuthorizeHandler(s.GetWorkspaceLimits, api2.ViewerRole))
	workspacesGroup.GET("/byid/:workspace_id", httpserver2.AuthorizeHandler(s.GetWorkspaceByID, api2.InternalRole))
	workspacesGroup.GET("", httpserver2.AuthorizeHandler(s.ListWorkspaces, api2.ViewerRole))
	workspacesGroup.GET("/:workspace_id", httpserver2.AuthorizeHandler(s.GetWorkspace, api2.ViewerRole))
	workspacesGroup.GET("/byname/:workspace_name", httpserver2.AuthorizeHandler(s.GetWorkspaceByName, api2.ViewerRole))

	organizationGroup := v1Group.Group("/organization")
	organizationGroup.GET("", httpserver2.AuthorizeHandler(s.ListOrganization, api2.KaytuAdminRole))
	organizationGroup.POST("", httpserver2.AuthorizeHandler(s.CreateOrganization, api2.KaytuAdminRole))
	organizationGroup.DELETE("/:organizationId", httpserver2.AuthorizeHandler(s.DeleteOrganization, api2.KaytuAdminRole))

	costEstimatorGroup := v1Group.Group("/costestimator")
	costEstimatorGroup.GET("/aws", httpserver2.AuthorizeHandler(s.GetAwsCost, api2.ViewerRole))
	costEstimatorGroup.GET("/azure", httpserver2.AuthorizeHandler(s.GetAzureCost, api2.ViewerRole))
}

func (s *Server) Start(ctx context.Context) error {
	go s.StateManager.StartReconciler(ctx)

	s.e.Logger.SetLevel(log.DEBUG)
	s.e.Logger.Infof("workspace service is started on %s", s.cfg.Http.Address)
	return s.e.Start(s.cfg.Http.Address)
}

func (s *Server) getBootstrapStatus(ws *db2.Workspace) (api.BootstrapStatusResponse, error) {
	resp := api.BootstrapStatusResponse{
		MinRequiredConnections: 3,
		WorkspaceCreationStatus: api.BootstrapProgress{
			Total: 2,
		},
		DiscoveryStatus: api.BootstrapProgress{
			Total: 4,
		},
		AnalyticsStatus: api.BootstrapProgress{
			Total: 4,
		},
		ComplianceStatus: api.BootstrapProgress{
			Total: int64(0),
		},
	}

	hctx := &httpclient.Context{UserRole: api2.InternalRole}
	schedulerURL := strings.ReplaceAll(s.cfg.Scheduler.BaseURL, "%NAMESPACE%", s.cfg.KaytuOctopusNamespace)
	schedulerClient := client3.NewSchedulerServiceClient(schedulerURL)

	if ws.Status == api.StateID_Provisioning {
		if !ws.IsBootstrapInputFinished {
			return resp, nil
		}
		resp.WorkspaceCreationStatus.Done = 1

		if !ws.IsCreated {
			return resp, nil
		}
		resp.WorkspaceCreationStatus.Done = 2

		status, err := schedulerClient.GetDescribeAllJobsStatus(hctx)
		if err != nil {
			return resp, err
		}

		if status != nil {
			switch *status {
			case api3.DescribeAllJobsStatusNoJobToRun:
				resp.DiscoveryStatus.Done = 1
			case api3.DescribeAllJobsStatusJobsRunning:
				resp.DiscoveryStatus.Done = 2
			case api3.DescribeAllJobsStatusJobsFinished:
				resp.DiscoveryStatus.Done = 3
			case api3.DescribeAllJobsStatusResourcesPublished:
				resp.DiscoveryStatus.Done = 4
			}
		}

		if ws.AnalyticsJobID > 0 {
			resp.AnalyticsStatus.Done = 1
			job, err := schedulerClient.GetAnalyticsJob(hctx, ws.AnalyticsJobID)
			if err != nil {
				return resp, err
			}
			if job != nil {
				switch job.Status {
				case api5.JobCreated:
					resp.AnalyticsStatus.Done = 2
				case api5.JobInProgress:
					resp.AnalyticsStatus.Done = 3
				case api5.JobCompleted, api5.JobCompletedWithFailure:
					resp.AnalyticsStatus.Done = 4
				}
			}
		}
	} else {
		resp.WorkspaceCreationStatus.Done = resp.WorkspaceCreationStatus.Total
		resp.ComplianceStatus.Done = resp.ComplianceStatus.Total
		resp.DiscoveryStatus.Done = resp.DiscoveryStatus.Total
		resp.AnalyticsStatus.Done = resp.AnalyticsStatus.Total
	}

	return resp, nil
}

// GetBootstrapStatus godoc
//
//	@Summary	Get bootstrap status
//	@Security	BearerToken
//	@Tags		workspace
//	@Accept		json
//	@Produce	json
//	@Param		workspace_name	path		string	true	"Workspace Name"
//	@Success	200				{object}	api.BootstrapStatusResponse
//	@Router		/workspace/api/v1/bootstrap/{workspace_name} [get]
func (s *Server) GetBootstrapStatus(c echo.Context) error {
	workspaceName := c.Param("workspace_name")
	ws, err := s.db.GetWorkspaceByName(workspaceName)
	if err != nil {
		return err
	}

	if err := s.CheckRoleInWorkspace(c, &ws.ID, ws.OwnerId, workspaceName); err != nil {
		return err
	}

	if ws == nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("workspace not found"))
	}

	resp, err := s.getBootstrapStatus(ws)
	if err != nil {
		return err
	}

	limits := api.GetLimitsByTier(ws.Tier)

	resp.MinRequiredConnections = 3
	resp.MaxConnections = limits.MaxConnections
	resp.ConnectionCount = make(map[source.Type]int64)
	return c.JSON(http.StatusOK, resp)
}

func (s *Server) GetWorkspace(c echo.Context) error {
	id := c.Param("workspace_id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "workspace id is empty")
	}

	workspace, err := s.db.GetWorkspace(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
		}
		c.Logger().Errorf("find workspace: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, ErrInternalServer)
	}

	if err := s.CheckRoleInWorkspace(c, &workspace.ID, workspace.OwnerId, workspace.Name); err != nil {
		return err
	}

	version := "unspecified"
	var kaytuVersionConfig corev1.ConfigMap
	err = s.kubeClient.Get(c.Request().Context(), k8sclient.ObjectKey{
		Namespace: workspace.ID,
		Name:      "kaytu-version",
	}, &kaytuVersionConfig)
	if err == nil {
		version = kaytuVersionConfig.Data["version"]
	} else {
		fmt.Printf("failed to load version due to %v\n", err)
	}

	return c.JSON(http.StatusOK, api.WorkspaceResponse{
		Workspace: workspace.ToAPI(),
		Version:   version,
	})
}

func (s *Server) GetWorkspaceByName(c echo.Context) error {
	name := c.Param("workspace_name")
	if name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "workspace name is empty")
	}

	workspace, err := s.db.GetWorkspaceByName(name)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
		}
		c.Logger().Errorf("find workspace: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, ErrInternalServer)
	}

	if err := s.CheckRoleInWorkspace(c, &workspace.ID, workspace.OwnerId, name); err != nil {
		return err
	}

	version := "unspecified"
	var kaytuVersionConfig corev1.ConfigMap
	err = s.kubeClient.Get(c.Request().Context(), k8sclient.ObjectKey{
		Namespace: workspace.ID,
		Name:      "kaytu-version",
	}, &kaytuVersionConfig)
	if err == nil {
		version = kaytuVersionConfig.Data["version"]
	} else {
		fmt.Printf("failed to load version due to %v\n", err)
	}

	return c.JSON(http.StatusOK, api.WorkspaceResponse{
		Workspace: workspace.ToAPI(),
		Version:   version,
	})
}

// ListWorkspaces godoc
//
//	@Summary		List all workspaces with owner id
//	@Description	Returns all workspaces with owner id
//	@Security		BearerToken
//	@Tags			workspace
//	@Accept			json
//	@Produce		json
//	@Success		200	{array}	api.WorkspaceResponse
//	@Router			/workspace/api/v1/workspaces [get]
func (s *Server) ListWorkspaces(c echo.Context) error {
	var resp authapi.GetRoleBindingsResponse
	var err error

	userId := httpserver2.GetUserID(c)

	if userId != api2.GodUserID {
		resp, err = s.authClient.GetUserRoleBindings(httpclient.FromEchoContext(c))
		if err != nil {
			return fmt.Errorf("GetUserRoleBindings: %v", err)
		}
	}

	dbWorkspaces, err := s.db.ListWorkspaces()
	if err != nil {
		return fmt.Errorf("ListWorkspaces: %v", err)
	}

	workspaces := make([]*api.WorkspaceResponse, 0)
	for _, workspace := range dbWorkspaces {
		hasRoleInWorkspace := false
		if userId != api2.GodUserID {
			for _, rb := range resp.RoleBindings {
				if rb.WorkspaceID == workspace.ID {
					hasRoleInWorkspace = true
				}
			}
			if resp.GlobalRoles != nil {
				hasRoleInWorkspace = true
			}
		} else {
			// god has role in everything
			hasRoleInWorkspace = true
		}

		if workspace.OwnerId != nil && *workspace.OwnerId == "kaytu|owner|all" {
			hasRoleInWorkspace = true
		}

		if workspace.OwnerId == nil || (*workspace.OwnerId != userId && !hasRoleInWorkspace) {
			continue
		}

		version := "unspecified"

		if workspace.IsCreated {
			var kaytuVersionConfig corev1.ConfigMap
			err = s.kubeClient.Get(c.Request().Context(), k8sclient.ObjectKey{
				Namespace: s.cfg.KaytuOctopusNamespace,
				Name:      "kaytu-version",
			}, &kaytuVersionConfig)
			if err == nil {
				version = kaytuVersionConfig.Data["version"]
			} else {
				fmt.Printf("failed to load version due to %v\n", err)
			}
		}

		workspaces = append(workspaces, &api.WorkspaceResponse{
			Workspace: workspace.ToAPI(),
			Version:   version,
		})
	}
	return c.JSON(http.StatusOK, workspaces)
}

// GetCurrentWorkspace godoc
//
//	@Summary		List all workspaces with owner id
//	@Description	Returns all workspaces with owner id
//	@Security		BearerToken
//	@Tags			workspace
//	@Accept			json
//	@Produce		json
//	@Success		200	{object}	api.WorkspaceResponse
//	@Router			/workspace/api/v1/workspace/current [get]
func (s *Server) GetCurrentWorkspace(c echo.Context) error {
	wsName := httpserver2.GetWorkspaceName(c)

	workspace, err := s.db.GetWorkspaceByName(wsName)
	if err != nil {
		return fmt.Errorf("ListWorkspaces: %v", err)
	}

	version := "unspecified"
	var kaytuVersionConfig corev1.ConfigMap
	err = s.kubeClient.Get(c.Request().Context(), k8sclient.ObjectKey{
		Namespace: workspace.ID,
		Name:      "kaytu-version",
	}, &kaytuVersionConfig)
	if err == nil {
		version = kaytuVersionConfig.Data["version"]
	} else {
		fmt.Printf("failed to load version due to %v\n", err)
	}

	return c.JSON(http.StatusOK, api.WorkspaceResponse{
		Workspace: workspace.ToAPI(),
		Version:   version,
	})
}

func (s *Server) ChangeOwnership(c echo.Context) error {
	userID := httpserver2.GetUserID(c)
	workspaceID := c.Param("workspace_id")

	var request api.ChangeWorkspaceOwnershipRequest
	if err := c.Bind(&request); err != nil {
		c.Logger().Errorf("bind the request: %v", err)
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}
	if workspaceID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "workspace id is empty")
	}

	w, err := s.db.GetWorkspace(workspaceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
		}
		return err
	}

	if *w.OwnerId != userID {
		return echo.NewHTTPError(http.StatusForbidden, "operation is forbidden")
	}

	err = s.db.UpdateWorkspaceOwner(workspaceID, request.NewOwnerUserID)
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}

func (s *Server) ChangeOrganization(c echo.Context) error {
	workspaceID := c.Param("workspace_id")

	var request api.ChangeWorkspaceOrganizationRequest
	if err := c.Bind(&request); err != nil {
		c.Logger().Errorf("bind the request: %v", err)
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}
	if workspaceID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "workspace id is empty")
	}

	_, err := s.db.GetWorkspace(workspaceID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
		}
		return err
	}

	_, err = s.db.GetOrganization(request.NewOrgID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "organization not found")
		}
		return err
	}

	err = s.db.UpdateWorkspaceOrganization(workspaceID, request.NewOrgID)
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusOK)
}

// GetWorkspaceLimits godoc
//
//	@Summary	Get workspace limits
//	@Security	BearerToken
//	@Tags		workspace
//	@Accept		json
//	@Produce	json
//	@Param		workspace_name	path		string	true	"Workspace Name"
//	@Param		ignore_usage	query		bool	false	"Ignore usage"
//	@Success	200				{object}	api.WorkspaceLimitsUsage
//	@Router		/workspace/api/v1/workspaces/limits/{workspace_name} [get]
func (s *Server) GetWorkspaceLimits(c echo.Context) error {
	var response api.WorkspaceLimitsUsage

	workspaceName := c.Param("workspace_name")
	ignoreUsage := c.QueryParam("ignore_usage")

	dbWorkspace, err := s.db.GetWorkspaceByName(workspaceName)
	if err != nil {
		return err
	}

	if err := s.CheckRoleInWorkspace(c, &dbWorkspace.ID, dbWorkspace.OwnerId, workspaceName); err != nil {
		return err
	}

	if ignoreUsage != "true" {
		ectx := httpclient.FromEchoContext(c)
		ectx.UserRole = api2.AdminRole
		resp, err := s.authClient.GetWorkspaceRoleBindings(ectx, dbWorkspace.ID)
		if err != nil {
			return fmt.Errorf("GetWorkspaceRoleBindings: %v", err)
		}
		response.CurrentUsers = int64(len(resp))

		inventoryURL := strings.ReplaceAll(s.cfg.Inventory.BaseURL, "%NAMESPACE%", s.cfg.KaytuOctopusNamespace)
		inventoryClient := client2.NewInventoryServiceClient(inventoryURL)
		resourceCount, err := inventoryClient.CountResources(httpclient.FromEchoContext(c))
		response.CurrentResources = resourceCount

		onboardURL := strings.ReplaceAll(s.cfg.Onboard.BaseURL, "%NAMESPACE%", s.cfg.KaytuOctopusNamespace)
		onboardClient := client.NewOnboardServiceClient(onboardURL)
		count, err := onboardClient.CountSources(httpclient.FromEchoContext(c), source.Nil)
		response.CurrentConnections = count
	}

	limits := api.GetLimitsByTier(dbWorkspace.Tier)
	response.MaxUsers = limits.MaxUsers
	response.MaxConnections = limits.MaxConnections
	response.MaxResources = limits.MaxResources
	response.ID = dbWorkspace.ID
	response.Name = dbWorkspace.Name
	return c.JSON(http.StatusOK, response)
}

func (s *Server) GetWorkspaceByID(c echo.Context) error {
	workspaceID := c.Param("workspace_id")

	dbWorkspace, err := s.db.GetWorkspace(workspaceID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, dbWorkspace.ToAPI())
}

func (s *Server) CreateOrganization(c echo.Context) error {
	var request api.Organization
	if err := c.Bind(&request); err != nil {
		return err
	}

	dbOrg := db.Organization{
		CompanyName:  request.CompanyName,
		Url:          request.Url,
		Address:      request.Address,
		City:         request.City,
		State:        request.State,
		Country:      request.Country,
		ContactPhone: request.ContactPhone,
		ContactEmail: request.ContactEmail,
		ContactName:  request.ContactName,
	}
	err := s.db.CreateOrganization(&dbOrg)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusCreated, dbOrg.ToAPI())
}

func (s *Server) ListOrganization(c echo.Context) error {
	orgs, err := s.db.ListOrganizations()
	if err != nil {
		return err
	}

	var apiOrg []api.Organization
	for _, org := range orgs {
		apiOrg = append(apiOrg, org.ToAPI())
	}
	return c.JSON(http.StatusCreated, apiOrg)
}

func (s *Server) DeleteOrganization(c echo.Context) error {
	organizationIDStr := c.Param("organizationId")
	organizationID, err := strconv.ParseInt(organizationIDStr, 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid organization ID")
	}
	_, err = s.db.GetOrganization(uint(organizationID))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Organization not found")
		}
		return err
	}

	err = s.db.DeleteOrganization(uint(organizationID))
	if err != nil {
		return err
	}

	return c.NoContent(http.StatusAccepted)
}
