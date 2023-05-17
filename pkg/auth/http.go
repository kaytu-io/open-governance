package auth

import (
	"context"
	"crypto/rsa"
	"crypto/sha512"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/kaytu-io/kaytu-util/pkg/email"
	"gitlab.com/keibiengine/keibi-engine/pkg/internal/httpclient"
	"gitlab.com/keibiengine/keibi-engine/pkg/internal/httpserver"
	metadataClient "gitlab.com/keibiengine/keibi-engine/pkg/metadata/client"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gitlab.com/keibiengine/keibi-engine/pkg/metadata/models"

	"gitlab.com/keibiengine/keibi-engine/pkg/auth/db"

	"github.com/golang-jwt/jwt"
	"gitlab.com/keibiengine/keibi-engine/pkg/auth/auth0"

	"gitlab.com/keibiengine/keibi-engine/pkg/workspace/client"

	"github.com/labstack/echo/v4"
	"gitlab.com/keibiengine/keibi-engine/pkg/auth/api"
	"go.uber.org/zap"
)

var (
	//go:embed email/invite.html
	inviteEmailTemplate string
)

type httpRoutes struct {
	logger          *zap.Logger
	emailService    email.Service
	workspaceClient client.WorkspaceServiceClient
	auth0Service    *auth0.Service
	metadataBaseUrl string
	keibiPrivateKey *rsa.PrivateKey
	db              db.Database
}

func (r *httpRoutes) Register(e *echo.Echo) {
	v1 := e.Group("/api/v1")

	v1.PUT("/user/role/binding", httpserver.AuthorizeHandler(r.PutRoleBinding, api.AdminRole))
	v1.DELETE("/user/role/binding", httpserver.AuthorizeHandler(r.DeleteRoleBinding, api.AdminRole))
	v1.GET("/user/role/bindings", httpserver.AuthorizeHandler(r.GetRoleBindings, api.EditorRole))
	v1.GET("/user/:user_id/workspace/membership", httpserver.AuthorizeHandler(r.GetWorkspaceMembership, api.AdminRole))
	v1.GET("/workspace/role/bindings", httpserver.AuthorizeHandler(r.GetWorkspaceRoleBindings, api.AdminRole))
	v1.GET("/users", httpserver.AuthorizeHandler(r.GetUsers, api.EditorRole))
	v1.GET("/user/:user_id", httpserver.AuthorizeHandler(r.GetUserDetails, api.EditorRole))
	v1.POST("/invite", httpserver.AuthorizeHandler(r.Invite, api.AdminRole))
	v1.POST("/user/invite", httpserver.AuthorizeHandler(r.Invite, api.AdminRole))
	v1.DELETE("/user/invite", httpserver.AuthorizeHandler(r.DeleteInvitation, api.AdminRole))

	v1.POST("/key/create", httpserver.AuthorizeHandler(r.CreateAPIKey, api.AdminRole))
	v1.GET("/keys", httpserver.AuthorizeHandler(r.ListAPIKeys, api.EditorRole))
	v1.POST("/key/:id/suspend", httpserver.AuthorizeHandler(r.SuspendAPIKey, api.AdminRole))
	v1.POST("/key/:id/activate", httpserver.AuthorizeHandler(r.ActivateAPIKey, api.AdminRole))
	v1.DELETE("/key/:id/delete", httpserver.AuthorizeHandler(r.DeleteAPIKey, api.AdminRole))
	v1.GET("/key/:id", httpserver.AuthorizeHandler(r.GetAPIKey, api.EditorRole))

	v1.GET("/role/:roleName/users", httpserver.AuthorizeHandler(r.GetRoleUsers, api.AdminRole))
	v1.GET("/role/:roleName/keys", httpserver.AuthorizeHandler(r.GetRoleKeys, api.AdminRole))
	v1.POST("/key/role", httpserver.AuthorizeHandler(r.UpdateKeyRole, api.AdminRole))
	v1.GET("/roles", httpserver.AuthorizeHandler(r.ListRoles, api.EditorRole))
	v1.GET("/roles/:roleName", httpserver.AuthorizeHandler(r.RoleDetails, api.EditorRole))
}

// ListRoles godoc
//
//	@Summary		Get Roles
//	@Description	Gets a list of roles in a workspace and their descriptions and number of users.
//	@Security		BearerToken
//	@Tags			roles
//	@Produce		json
//	@Success		200	{object}	[]api.RolesListResponse
//	@Router			/auth/api/v1/roles [get]
func (r *httpRoutes) ListRoles(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)
	users, err := r.auth0Service.SearchUsers(workspaceID, nil, nil, nil)
	if err != nil {
		return err
	}

	var AdminCount int
	var ViewerCount int
	var EditorCount int

	for _, u := range users {
		role := u.AppMetadata.WorkspaceAccess[workspaceID]
		if role == api.AdminRole {
			AdminCount++
		} else if role == api.ViewerRole {
			ViewerCount++
		} else if role == api.EditorRole {
			EditorCount++
		}
	}

	descriptions := map[api.Role]string{
		api.AdminRole:  "The Administrator role is a super user role with all of the capabilities that can be assigned to a role, and its enables access to all data & configuration on a Kaytu Workspace. You cannot edit or delete the Administrator role.",
		api.EditorRole: "Provide full access to manage all capabilities in a workspace, with three exceptions: Changing Workspace Settings, Modifying Integrations, and making changes to user access controls.",
		api.ViewerRole: "View all resources, but does not allow you to make any changes or trigger any action (running asset discovery).",
	}

	var res = []api.RolesListResponse{
		{
			RoleName:    api.AdminRole,
			Description: descriptions[api.AdminRole],
			UserCount:   AdminCount,
		},
		{
			RoleName:    api.EditorRole,
			Description: descriptions[api.EditorRole],
			UserCount:   EditorCount,
		},
		{
			RoleName:    api.ViewerRole,
			Description: descriptions[api.ViewerRole],
			UserCount:   ViewerCount,
		},
	}
	return ctx.JSON(http.StatusOK, res)
}

// RoleDetails godoc
//
//	@Summary		Get Role Details
//	@Description	Gets the details of the Role, including the description, number of users and list of those users.
//	@Security		BearerToken
//	@Tags			roles
//	@Produce		json
//	@Param			roleName	path		string	true	"roleName"
//	@Success		200			{object}	api.RoleDetailsResponse
//	@Router			/auth/api/v1/roles/{roleName} [get]
func (r *httpRoutes) RoleDetails(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)
	role := api.Role(ctx.Param("roleName"))
	users, err := r.auth0Service.SearchUsers(workspaceID, nil, nil, nil)
	if err != nil {
		return err
	}

	var roleCount int
	var roleUsers []api.GetUsersResponse

	for _, u := range users {
		r := u.AppMetadata.WorkspaceAccess[workspaceID]
		if role == r {
			roleCount++
			roleUsers = append(roleUsers, api.GetUsersResponse{
				UserID:        u.UserId,
				UserName:      u.Name,
				Email:         u.Email,
				EmailVerified: u.EmailVerified,
				RoleName:      role,
			})
		}
	}
	descriptions := map[api.Role]string{
		api.AdminRole:  "The Administrator role is a super user role with all of the capabilities that can be assigned to a role, and its enables access to all data & configuration on a Kaytu Workspace. You cannot edit or delete the Administrator role.",
		api.EditorRole: "Provide full access to manage all capabilities in a workspace, with three exceptions: Changing Workspace Settings, Modifying Integrations, and making changes to user access controls.",
		api.ViewerRole: "View all resources, but does not allow you to make any changes or trigger any action (running asset discovery).",
	}
	var res = api.RoleDetailsResponse{
		RoleName:    role,
		Description: descriptions[role],
		UserCount:   roleCount,
		Users:       roleUsers,
	}
	return ctx.JSON(http.StatusOK, res)
}

func bindValidate(ctx echo.Context, i interface{}) error {
	if err := ctx.Bind(i); err != nil {
		return err
	}

	if err := ctx.Validate(i); err != nil {
		return err
	}

	return nil
}

// PutRoleBinding godoc
//
//	@Summary		Update User Access
//	@Description	User Access defines the roles of a user.
//	@Description	There are currently three roles (admin, editor, viewer).
//	@Description	User must exist before you can update its Role.
//	@Security		BearerToken
//	@Tags			users
//	@Produce		json
//	@Param			request	body		api.PutRoleBindingRequest	true	"Request Body"
//	@Success		200		{object}	nil
//	@Router			/auth/api/v1/user/role/binding [put]
func (r httpRoutes) PutRoleBinding(ctx echo.Context) error {
	var req api.PutRoleBindingRequest
	if err := bindValidate(ctx, &req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err)
	}

	workspaceID := httpserver.GetWorkspaceID(ctx)

	if httpserver.GetUserID(ctx) == req.UserID &&
		req.RoleName != api.AdminRole {
		return echo.NewHTTPError(http.StatusBadRequest, "admin user permission can't be modified by self")
	}
	// The WorkspaceManager service will call this API to set the AdminRole
	// for the admin user on behalf of him. Allow for the Admin to only set its
	// role to admin for that user case
	auth0User, err := r.auth0Service.GetUser(req.UserID)
	if err != nil {
		return err
	}

	if _, ok := auth0User.AppMetadata.WorkspaceAccess[workspaceID]; !ok {
		hctx := httpclient.FromEchoContext(ctx)
		metadataService := metadataClient.NewMetadataServiceClient(fmt.Sprintf(metadataBaseUrl, workspaceID))
		cnf, err := metadataService.GetConfigMetadata(hctx, models.MetadataKeyUserLimit)
		if err != nil {
			return err
		}
		maxUsers := cnf.GetValue().(int)

		users, err := r.auth0Service.SearchUsers(workspaceID, nil, nil, nil)
		if err != nil {
			return err
		}

		if len(users)+1 > maxUsers {
			return echo.NewHTTPError(http.StatusNotAcceptable, "cannot invite new user, max users reached")
		}
	}

	auth0User.AppMetadata.WorkspaceAccess[workspaceID] = req.RoleName
	err = r.auth0Service.PatchUserAppMetadata(req.UserID, auth0User.AppMetadata)
	if err != nil {
		return err
	}
	return ctx.NoContent(http.StatusOK)
}

// DeleteRoleBinding godoc
//
//	@Summary		Delete User Access
//	@Description	Deletes user access to the specified workspace.
//	@Security		BearerToken
//	@Tags			users
//	@Produce		json
//	@Param			userId	query		string	true	"userId"
//	@Success		200		{object}	nil
//	@Router			/auth/api/v1/user/role/binding [delete]
func (r httpRoutes) DeleteRoleBinding(ctx echo.Context) error {
	userId := ctx.QueryParam("userId")
	// The WorkspaceManager service will call this API to set the AdminRole
	// for the admin user on behalf of him. Allow for the Admin to only set its
	// role to admin for that user case
	if httpserver.GetUserID(ctx) == userId {
		return echo.NewHTTPError(http.StatusBadRequest, "admin user permission can't be modified by self")
	}

	workspaceID := httpserver.GetWorkspaceID(ctx)
	auth0User, err := r.auth0Service.GetUser(userId)
	if err != nil {
		return err
	}

	delete(auth0User.AppMetadata.WorkspaceAccess, workspaceID)
	err = r.auth0Service.PatchUserAppMetadata(userId, auth0User.AppMetadata)
	if err != nil {
		return err
	}
	return ctx.NoContent(http.StatusOK)
}

// GetRoleBindings godoc
//
//	@Summary		Get RoleBindings
//	@Description	Gets the roles binded to a user.
//	@Description	RoleBinding defines the roles and actions a user can perform. There are currently three roles (admin, editor, viewer).
//	@Security		BearerToken
//	@Tags			users
//	@Produce		json
//	@Success		200	{object}	api.GetRoleBindingsResponse
//	@Router			/auth/api/v1/user/role/bindings [get]
func (r *httpRoutes) GetRoleBindings(ctx echo.Context) error {
	userID := httpserver.GetUserID(ctx)

	var resp api.GetRoleBindingsResponse
	usr, err := r.auth0Service.GetUser(userID)
	if err != nil {
		r.logger.Warn("failed to get user from auth0 due to", zap.Error(err))
		return err
	}

	if usr != nil {
		for wsID, role := range usr.AppMetadata.WorkspaceAccess {
			resp.RoleBindings = append(resp.RoleBindings, api.UserRoleBinding{
				WorkspaceID: wsID,
				RoleName:    role,
			})
		}
		resp.GlobalRoles = usr.AppMetadata.GlobalAccess
	} else {
		r.logger.Warn("user not found in auth0", zap.String("externalID", userID))
	}
	return ctx.JSON(http.StatusOK, resp)
}

// GetWorkspaceMembership godoc
//
//	@Summary		User Workspaces
//	@Description	Returns a list of workspaces and the user role in it for the specified user.
//	@Security		BearerToken
//	@Tags			users
//	@Produce		json
//	@Param			userId	path		string	true	"userId"
//	@Success		200		{object}	api.GetRoleBindingsResponse
//	@Router			/auth/api/v1/user/{user_id}/workspace/membership [get]
func (r *httpRoutes) GetWorkspaceMembership(ctx echo.Context) error {
	hctx := httpclient.FromEchoContext(ctx)
	userID := ctx.Param("user_id")

	var resp []api.Membership
	usr, err := r.auth0Service.GetUser(userID)
	if err != nil {
		r.logger.Warn("failed to get user from auth0 due to", zap.Error(err))
		return err
	}

	if usr != nil {
		for wsID, role := range usr.AppMetadata.WorkspaceAccess {
			ws, err := r.workspaceClient.GetByID(hctx, wsID)
			if err != nil {
				r.logger.Warn("failed to get workspace due to", zap.Error(err))
				return err
			}

			resp = append(resp, api.Membership{
				WorkspaceID:   wsID,
				WorkspaceName: ws.Name,
				RoleName:      role,
				AssignedAt:    time.Time{}, //TODO- add assigned at
				LastActivity:  time.Time{}, //TODO- add assigned at
			})
		}
	} else {
		r.logger.Warn("user not found in auth0", zap.String("externalID", userID))
	}
	return ctx.JSON(http.StatusOK, resp)
}

// GetWorkspaceRoleBindings godoc
//
//	@Summary		Workspace user roleBindings.
//	@Description	RoleBinding defines the roles and actions a user can perform. There are currently three roles (admin, editor, viewer). The workspace path is based on the DNS such as (workspace1.app.keibi.io)
//	@Security		BearerToken
//	@Tags			users
//	@Produce		json
//	@Success		200	{object}	api.GetWorkspaceRoleBindingResponse
//	@Router			/auth/api/v1/workspace/role/bindings [get]
func (r *httpRoutes) GetWorkspaceRoleBindings(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)
	users, err := r.auth0Service.SearchUsersByWorkspace(workspaceID)
	if err != nil {
		return err
	}

	var resp api.GetWorkspaceRoleBindingResponse
	for _, u := range users {
		status := api.InviteStatus_PENDING
		if u.EmailVerified {
			status = api.InviteStatus_ACCEPTED
		}

		resp = append(resp, api.WorkspaceRoleBinding{
			UserID:       u.UserId,
			UserName:     u.Name,
			Email:        u.Email,
			RoleName:     u.AppMetadata.WorkspaceAccess[workspaceID],
			Status:       status,
			LastActivity: u.LastLogin,
			CreatedAt:    u.CreatedAt,
		})
	}
	return ctx.JSON(http.StatusOK, resp)
}

// GetUsers godoc
//
//	@Summary		Get Users
//	@Description	Gets a list of users with specified filters (filters are optional).
//	@Security		BearerToken
//	@Tags			users
//	@Produce		json
//	@Param			request	body		api.GetUsersRequest	true	"Request Body"
//	@Success		200		{object}	[]api.GetUsersResponse
//	@Router			/auth/api/v1/users [get]
func (r *httpRoutes) GetUsers(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)
	var req api.GetUsersRequest
	if err := ctx.Bind(&req); err != nil {
		ctx.Logger().Errorf("bind the request: %v", err)
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}
	users, err := r.auth0Service.SearchUsers(workspaceID, req.Email, req.EmailVerified, req.RoleName)
	if err != nil {
		return err
	}
	var resp []api.GetUsersResponse
	for _, u := range users {

		resp = append(resp, api.GetUsersResponse{
			UserID:        u.UserId,
			UserName:      u.Name,
			Email:         u.Email,
			EmailVerified: u.EmailVerified,
			RoleName:      u.AppMetadata.WorkspaceAccess[workspaceID],
		})
	}
	return ctx.JSON(http.StatusOK, resp)
}

// GetUserDetails godoc
//
//	@Summary		Get User details
//	@Description	Get user details by user id.
//	@Security		BearerToken
//	@Tags			users
//	@Produce		json
//	@Param			userId	path		string	true	"userId"
//	@Success		200		{object}	api.GetUserResponse
//	@Router			/auth/api/v1/user/{user_id} [get]
func (r *httpRoutes) GetUserDetails(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)
	userID := ctx.Param("user_id")
	user, err := r.auth0Service.GetUser(userID)
	if err != nil {
		return err
	}
	hasARole := false
	for ws, _ := range user.AppMetadata.WorkspaceAccess {
		if ws == workspaceID {
			hasARole = true
			break
		}
	}
	if hasARole == false {
		return echo.NewHTTPError(http.StatusBadRequest, "The user is not in the specified workspace.")
	}
	status := api.InviteStatus_PENDING
	if user.EmailVerified {
		status = api.InviteStatus_ACCEPTED
	}
	resp := api.GetUserResponse{
		UserID:        user.UserId,
		UserName:      user.Name,
		Email:         user.Email,
		EmailVerified: user.EmailVerified,
		Status:        status,
		LastActivity:  user.LastLogin,
		CreatedAt:     user.CreatedAt,
		Blocked:       user.Blocked,
		RoleName:      user.AppMetadata.WorkspaceAccess[workspaceID],
	}

	return ctx.JSON(http.StatusOK, resp)

}

// Invite godoc
//
//	@Summary		Invite User
//	@Description	Invites a user to a workspace with defined role.
//	@Description	by sending an email to the specified email address.
//	@Description	The user will be found by the email address.
//	@Security		BearerToken
//	@Tags			users
//	@Produce		json
//	@Param			request	body		api.InviteRequest	true	"Request Body"
//	@Success		200		{object}	nil
//	@Router			/auth/api/v1/user/invite [post]
func (r *httpRoutes) Invite(ctx echo.Context) error {
	var req api.InviteRequest
	if err := bindValidate(ctx, &req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err)
	}
	workspaceID := httpserver.GetWorkspaceID(ctx)

	hctx := httpclient.FromEchoContext(ctx)

	metadataService := metadataClient.NewMetadataServiceClient(fmt.Sprintf(metadataBaseUrl, workspaceID))
	cnf, err := metadataService.GetConfigMetadata(hctx, models.MetadataKeyAllowInvite)
	if err != nil {
		return err
	}

	allowInvite := cnf.GetValue().(bool)
	if !allowInvite {
		return echo.NewHTTPError(http.StatusNotAcceptable, "invite not allowed")
	}

	cnf, err = metadataService.GetConfigMetadata(hctx, models.MetadataKeyUserLimit)
	if err != nil {
		return err
	}
	maxUsers := cnf.GetValue().(int)

	users, err := r.auth0Service.SearchUsers(workspaceID, nil, nil, nil)
	if err != nil {
		return err
	}
	if len(users)+1 > maxUsers {
		return echo.NewHTTPError(http.StatusNotAcceptable, "cannot invite new user, max users reached")
	}

	cnf, err = metadataService.GetConfigMetadata(hctx, models.MetadataKeyAllowedEmailDomains)
	if err != nil {
		return err
	}

	if allowedEmailDomains, ok := cnf.GetValue().([]string); ok {
		passed := false
		if len(allowedEmailDomains) > 0 {
			for _, domain := range allowedEmailDomains {
				if strings.HasSuffix(req.Email, domain) {
					passed = true
				}
			}
		} else {
			passed = true
		}

		if !passed {
			return echo.NewHTTPError(http.StatusNotAcceptable, "email domain not allowed")
		}
	} else {
		fmt.Printf("failed to parse allowed domains, type: %s, value: %v", reflect.TypeOf(cnf.GetValue()).Name(), cnf.GetValue())
	}

	us, err := r.auth0Service.SearchByEmail(req.Email)
	if err != nil {
		return err
	}

	if len(us) > 0 {
		auth0User := us[0]
		if auth0User.AppMetadata.WorkspaceAccess == nil {
			auth0User.AppMetadata.WorkspaceAccess = map[string]api.Role{}
		}
		auth0User.AppMetadata.WorkspaceAccess[workspaceID] = req.RoleName
		err = r.auth0Service.PatchUserAppMetadata(auth0User.UserId, auth0User.AppMetadata)
		if err != nil {
			return err
		}

		emailContent := inviteEmailTemplate
		err = r.emailService.SendEmail(context.Background(), req.Email, emailContent)
		if err != nil {
			return err
		}
	} else {
		user, err := r.auth0Service.CreateUser(req.Email, workspaceID, req.RoleName)
		if err != nil {
			return err
		}

		resp, err := r.auth0Service.CreatePasswordChangeTicket(user.UserId)
		if err != nil {
			return err
		}

		emailContent := inviteEmailTemplate
		emailContent = strings.ReplaceAll(emailContent, "{{ url }}", resp.Ticket)
		err = r.emailService.SendEmail(context.Background(), req.Email, emailContent)
		if err != nil {
			return err
		}
	}

	return ctx.NoContent(http.StatusOK)
}

// DeleteInvitation godoc
//
//	@Summary		Delete Invitation
//	@Description	Deletes user access to the specified workspace.
//	@Security		BearerToken
//	@Tags			users
//	@Produce		json
//	@Param			userId	query		string	true	"userId"
//	@Success		200		{object}	nil
//	@Router			/auth/api/v1/user/invite [delete]
func (r *httpRoutes) DeleteInvitation(ctx echo.Context) error {
	userId := ctx.QueryParam("userId")
	if httpserver.GetUserID(ctx) == userId {
		return echo.NewHTTPError(http.StatusBadRequest, "admin user permission can't be modified by self")
	}

	workspaceID := httpserver.GetWorkspaceID(ctx)
	auth0User, err := r.auth0Service.GetUser(userId)
	if err != nil {
		return err
	}

	delete(auth0User.AppMetadata.WorkspaceAccess, workspaceID)
	err = r.auth0Service.PatchUserAppMetadata(userId, auth0User.AppMetadata)
	if err != nil {
		return err
	}
	return ctx.NoContent(http.StatusOK)
}

// CreateAPIKey godoc
//
//	@Summary		Creates Workspace Key
//	@Description	Creates workspace key for the defined role with the defined name.
//	@Security		BearerToken
//	@Tags			keys
//	@Produce		json
//	@Param			request	body		api.CreateAPIKeyRequest	true	"Request Body"
//	@Success		200		{object}	api.CreateAPIKeyResponse
//	@Router			/auth/api/v1/key/create [post]
func (r *httpRoutes) CreateAPIKey(ctx echo.Context) error {
	userID := httpserver.GetUserID(ctx)
	workspaceID := httpserver.GetWorkspaceID(ctx)
	hctx := httpclient.FromEchoContext(ctx)
	metadataService := metadataClient.NewMetadataServiceClient(fmt.Sprintf(metadataBaseUrl, workspaceID))

	cnf, err := metadataService.GetConfigMetadata(hctx, models.MetadataKeyWorkspaceKeySupport)
	if err != nil {
		return err
	}
	keySupport := cnf.GetValue().(bool)
	if !keySupport {
		return echo.NewHTTPError(http.StatusNotAcceptable, "keys are not supported in this workspace")
	}

	var req api.CreateAPIKeyRequest
	if err := bindValidate(ctx, &req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err)
	}

	usr, err := r.auth0Service.GetUser(userID)
	if err != nil {
		return err
	}

	if usr == nil {
		return errors.New("failed to find user in auth0")
	}

	u := userClaim{
		WorkspaceAccess: map[string]api.Role{
			workspaceID: req.RoleName,
		},
		GlobalAccess:   nil,
		Email:          usr.Email,
		ExternalUserID: usr.UserId,
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, &u).SignedString(r.keibiPrivateKey)
	if err != nil {
		return err
	}

	masked := fmt.Sprintf("%s...%s", token[:3], token[len(token)-2:])

	hash := sha512.New()
	_, err = hash.Write([]byte(token))
	if err != nil {
		return err
	}
	keyHash := hex.EncodeToString(hash.Sum(nil))

	currentKeyCount, err := r.db.CountApiKeys(workspaceID)
	if err != nil {
		return err
	}

	cnf, err = metadataService.GetConfigMetadata(hctx, models.MetadataKeyWorkspaceMaxKeys)
	if err != nil {
		return err
	}
	maxKeys := cnf.GetValue().(int)
	if currentKeyCount+1 > int64(maxKeys) {
		return echo.NewHTTPError(http.StatusNotAcceptable, "maximum number of keys in workspace reached")
	}

	apikey := db.ApiKey{
		Name:          req.Name,
		Role:          req.RoleName,
		CreatorUserID: userID,
		WorkspaceID:   workspaceID,
		Active:        true,
		Revoked:       false,
		MaskedKey:     masked,
		KeyHash:       keyHash,
	}
	err = r.db.AddApiKey(&apikey)
	if err != nil {
		return err
	}
	key, err := r.db.GetApiKey(workspaceID, uint(apikey.ID))
	if err != nil {
		return err
	}
	return ctx.JSON(http.StatusOK, api.CreateAPIKeyResponse{
		ID:        apikey.ID,
		Name:      key.Name,
		Active:    key.Active,
		CreatedAt: key.CreatedAt,
		RoleName:  key.Role,
		Token:     token,
	})
}

// DeleteAPIKey godoc
//
//	@Summary		Deletes Workspace Key
//	@Description	Deletes the specified workspace key by ID.
//	@Security		BearerToken
//	@Tags			keys
//	@Produce		json
//	@Param			id	path		string	true	"ID"
//	@Success		200	{object}	nil
//	@Router			/auth/api/v1/key/{id}/delete [delete]
func (r *httpRoutes) DeleteAPIKey(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)
	idStr := ctx.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return err
	}

	err = r.db.RevokeAPIKey(workspaceID, uint(id))
	if err != nil {
		return err
	}

	return ctx.NoContent(http.StatusOK)
}

// ListAPIKeys godoc
//
//	@Summary		Get Workspace Keys
//	@Description	Gets a list of available keys in a workspace.
//	@Security		BearerToken
//	@Tags			keys
//	@Produce		json
//	@Success		200	{object}	[]api.WorkspaceApiKey
//	@Router			/auth/api/v1/keys [get]
func (r *httpRoutes) ListAPIKeys(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)
	keys, err := r.db.ListApiKeys(workspaceID)
	if err != nil {
		return err
	}

	var resp []api.WorkspaceApiKey
	for _, key := range keys {
		resp = append(resp, api.WorkspaceApiKey{
			ID:            key.ID,
			CreatedAt:     key.CreatedAt,
			Name:          key.Name,
			RoleName:      key.Role,
			CreatorUserID: key.CreatorUserID,
			Active:        key.Active,
			MaskedKey:     key.MaskedKey,
		})
	}

	return ctx.JSON(http.StatusOK, resp)
}

// GetAPIKey godoc
//
//	@Summary		Get Workspace Key Details
//	@Description	Gets the details of a key in a workspace with specified ID.
//	@Security		BearerToken
//	@Tags			keys
//	@Produce		json
//	@Param			id	path		string	true	"ID"
//	@Success		200	{object}	api.WorkspaceApiKey
//	@Router			/auth/api/v1/key/{id} [get]
func (r *httpRoutes) GetAPIKey(ctx echo.Context) error {
	idStr := ctx.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return err
	}

	workspaceID := httpserver.GetWorkspaceID(ctx)
	key, err := r.db.GetApiKey(workspaceID, uint(id))
	if err != nil {
		return err
	}
	if key.ID == 0 {
		return echo.NewHTTPError(http.StatusNotFound, "api key not found")
	}

	resp := api.WorkspaceApiKey{
		ID:            key.ID,
		CreatedAt:     key.CreatedAt,
		UpdatedAt:     key.UpdatedAt,
		Name:          key.Name,
		RoleName:      key.Role,
		CreatorUserID: key.CreatorUserID,
		Active:        key.Active,
		MaskedKey:     key.MaskedKey,
	}

	return ctx.JSON(http.StatusOK, resp)
}

// SuspendAPIKey godoc
//
//	@Summary		Suspend Workspace Key
//	@Description	Suspends a key in the workspace with specified ID.
//	@Security		BearerToken
//	@Tags			keys
//	@Produce		json
//	@Param			id	path		string	true	"ID"
//	@Success		200	{object}	api.WorkspaceApiKey
//	@Router			/auth/api/v1/key/{id}/suspend [post]
func (r *httpRoutes) SuspendAPIKey(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)

	idStr := ctx.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return err
	}

	err = r.db.UpdateActiveAPIKey(workspaceID, uint(id), false)
	if err != nil {
		return err
	}

	key, err := r.db.GetApiKey(workspaceID, uint(id))
	if err != nil {
		return err
	}
	if key.ID == 0 {
		return echo.NewHTTPError(http.StatusNotFound, "api key not found")
	}

	resp := api.WorkspaceApiKey{
		ID:        key.ID,
		CreatedAt: key.CreatedAt,
		Name:      key.Name,
		RoleName:  key.Role,
		Active:    key.Active,
		MaskedKey: key.MaskedKey,
	}

	return ctx.JSON(http.StatusOK, resp)
}

// ActivateAPIKey godoc
//
//	@Summary		Activate Workspace Key
//	@Description	Activates a key in the workspace with specified ID.
//	@Security		BearerToken
//	@Tags			keys
//	@Produce		json
//	@Param			id	path		string	true	"ID"
//	@Success		200	{object}	api.WorkspaceApiKey
//	@Router			/auth/api/v1/key/{id}/activate [post]
func (r *httpRoutes) ActivateAPIKey(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)

	idStr := ctx.Param("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return err
	}

	err = r.db.UpdateActiveAPIKey(workspaceID, uint(id), true)
	if err != nil {
		return err
	}

	key, err := r.db.GetApiKey(workspaceID, uint(id))
	if err != nil {
		return err
	}
	if key.ID == 0 {
		return echo.NewHTTPError(http.StatusNotFound, "api key not found")
	}

	resp := api.WorkspaceApiKey{
		ID:        key.ID,
		CreatedAt: key.CreatedAt,
		Name:      key.Name,
		RoleName:  key.Role,
		Active:    key.Active,
		MaskedKey: key.MaskedKey,
	}

	return ctx.JSON(http.StatusOK, resp)
}

// GetRoleUsers godoc
//
//	@Summary		Lists Role Users
//	@Description	Returns a list of users in a workspace with the specified role.
//	@Security		BearerToken
//	@Tags			roles
//	@Produce		json
//	@Param			roleName	path		string	true	"roleName"
//	@Success		200			{object}	api.GetRoleUsersResponse
//	@Router			/auth/api/v1/role/{roleName}/users [get]
func (r *httpRoutes) GetRoleUsers(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)
	role := api.Role(ctx.Param("roleName"))
	users, err := r.auth0Service.SearchUsers(workspaceID, nil, nil, &role)
	if err != nil {
		return err
	}
	var resp api.GetRoleUsersResponse
	for _, u := range users {
		status := api.InviteStatus_PENDING
		if u.EmailVerified {
			status = api.InviteStatus_ACCEPTED
		}
		var workspaces []string
		for ws, r := range u.AppMetadata.WorkspaceAccess {
			if r == role {
				workspaces = append(workspaces, ws)
			}
		}

		resp = append(resp, api.RoleUser{
			UserID:        u.UserId,
			UserName:      u.Name,
			Email:         u.Email,
			EmailVerified: u.EmailVerified,
			RoleName:      role,
			Workspaces:    workspaces,
			Status:        status,
			LastActivity:  u.LastLogin,
			CreatedAt:     u.CreatedAt,
			Blocked:       u.Blocked,
		})
	}
	return ctx.JSON(http.StatusOK, resp)
}

// GetRoleKeys godoc
//
//	@Summary		Get Role Keys
//	@Description	Returns a list of keys in a workspace for the specified role.
//	@Security		BearerToken
//	@Tags			roles
//	@Produce		json
//	@Param			roleName	path		string	true	"roleName"
//	@Success		200			{object}	[]api.WorkspaceApiKey
//	@Router			/auth/api/v1/role/{roleName}/keys [get]
func (r *httpRoutes) GetRoleKeys(ctx echo.Context) error {
	role := api.Role(ctx.Param("roleName"))
	workspaceID := httpserver.GetWorkspaceID(ctx)
	keys, err := r.db.GetAPIKeysByRole(role, workspaceID)
	if err != nil {
		return err
	}

	var resp []api.WorkspaceApiKey
	for _, key := range keys {
		resp = append(resp, api.WorkspaceApiKey{
			ID:            key.ID,
			CreatedAt:     key.CreatedAt,
			UpdatedAt:     key.UpdatedAt,
			Name:          key.Name,
			RoleName:      key.Role,
			CreatorUserID: key.CreatorUserID,
			Active:        key.Active,
			MaskedKey:     key.MaskedKey,
		})
	}

	return ctx.JSON(http.StatusOK, resp)
}

// UpdateKeyRole godoc
//
//	@Summary		Update Workspace Key Role
//	@Description	Updates the role of the specified key in workspace.
//	@Security		BearerToken
//	@Tags			keys
//	@Produce		json
//	@Param			request	body		api.UpdateKeyRoleRequest	true	"Request Body"
//	@Success		200		{object}	api.WorkspaceApiKey
//	@Router			/auth/api/v1/key/role [post]
func (r *httpRoutes) UpdateKeyRole(ctx echo.Context) error {
	workspaceID := httpserver.GetWorkspaceID(ctx)

	var req api.UpdateKeyRoleRequest
	if err := ctx.Bind(&req); err != nil {
		ctx.Logger().Errorf("bind the request: %v", err)
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}

	err := r.db.UpdateAPIKeyRole(workspaceID, uint(req.ID), req.RoleName)
	if err != nil {
		return err
	}
	key, err := r.db.GetApiKey(workspaceID, uint(req.ID))
	if err != nil {
		return err
	}
	if key.ID == 0 {
		return echo.NewHTTPError(http.StatusNotFound, "api key not found")
	}

	resp := api.WorkspaceApiKey{
		ID:        key.ID,
		CreatedAt: key.CreatedAt,
		Name:      key.Name,
		RoleName:  key.Role,
		Active:    key.Active,
		MaskedKey: key.MaskedKey,
	}

	return ctx.JSON(http.StatusOK, resp)
}
