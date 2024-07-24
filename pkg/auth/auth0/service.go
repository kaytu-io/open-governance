package auth0

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/kaytu-io/kaytu-util/pkg/api"
	"io/ioutil"
	"math/rand"
	"net/http"
	url2 "net/url"
	"strconv"
)

type Service struct {
	domain       string
	clientID     string
	clientSecret string
	appClientID  string
	Connection   string
	InviteTTL    int

	token string
}

func New(domain, appClientID, clientID, clientSecret, connection string, inviteTTL int) *Service {
	return &Service{
		domain:       domain,
		appClientID:  appClientID,
		clientID:     clientID,
		clientSecret: clientSecret,
		Connection:   connection,
		InviteTTL:    inviteTTL,
		token:        "",
	}
}

func (a *Service) fillToken() error {
	url := fmt.Sprintf("%s/oauth/token", a.domain)
	req := TokenRequest{
		ClientId:     a.clientID,
		ClientSecret: a.clientSecret,
		Audience:     fmt.Sprintf("%s/api/v2/", a.domain),
		GrantType:    "client_credentials",
	}
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}

	res, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}

	if res.StatusCode != http.StatusOK {
		r, _ := ioutil.ReadAll(res.Body)
		str := ""
		if r != nil {
			str = string(r)
		}
		return fmt.Errorf("[fillToken] invalid status code: %d. res: %s", res.StatusCode, str)
	}

	r, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	var resp TokenResponse
	err = json.Unmarshal(r, &resp)
	if err != nil {
		return err
	}

	a.token = resp.AccessToken
	return nil
}

func (a *Service) GetUser(userID string) (*User, error) {
	if err := a.fillToken(); err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/v2/users/%s", a.domain, userID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+a.token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		r, _ := ioutil.ReadAll(res.Body)
		return nil, fmt.Errorf("[GetUser] invalid status code: %d, body=%s", res.StatusCode, string(r))
	}

	r, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var resp User
	err = json.Unmarshal(r, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (a *Service) SearchByEmail(email string) ([]User, error) {
	if err := a.fillToken(); err != nil {
		return nil, err
	}

	encoded := url2.Values{}
	encoded.Set("email", email)

	url := fmt.Sprintf("%s/api/v2/users-by-email?%s", a.domain, encoded.Encode())
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+a.token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		r, _ := ioutil.ReadAll(res.Body)
		return nil, fmt.Errorf("[SearchByEmail] invalid status code: %d, body=%s", res.StatusCode, string(r))
	}

	r, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var resp []User
	err = json.Unmarshal(r, &resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (a *Service) CreateUser(email, wsName string, role api.Role) (*User, error) {
	var defaultPass = "kaytu23@"
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	randPass := make([]rune, 10)
	for i := 0; i < 10; i++ {
		randPass[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	password := fmt.Sprintf("%s%s", defaultPass, string(randPass))

	usr := CreateUserRequest{
		Email:         email,
		EmailVerified: false,
		AppMetadata: Metadata{
			WorkspaceAccess: map[string]api.Role{
				wsName: role,
			},
			GlobalAccess: nil,
		},
		Password:   password,
		Connection: a.Connection,
	}

	if err := a.fillToken(); err != nil {
		return nil, err
	}

	body, err := json.Marshal(usr)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/v2/users", a.domain)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	fmt.Println("POST", url)
	fmt.Println(string(body))
	fmt.Println(body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+a.token)
	req.Header.Add("Content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		r, _ := ioutil.ReadAll(res.Body)
		return nil, fmt.Errorf("[CreateUser] invalid status code: %d, body=%s", res.StatusCode, string(r))
	}

	r, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var resp User
	err = json.Unmarshal(r, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (a *Service) DeleteUser(userId string) error {
	if err := a.fillToken(); err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v2/users/%s", a.domain, userId)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("[DeleteUser] could not delete user: %d", res.StatusCode)
	}
	return nil
}

func (a *Service) CreatePasswordChangeTicket(userId string) (*CreatePasswordChangeTicketResponse, error) {
	request := CreatePasswordChangeTicketRequest{
		UserId:   userId,
		ClientId: a.appClientID,
		TTLSec:   a.InviteTTL,
	}

	if err := a.fillToken(); err != nil {
		return nil, err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/v2/tickets/password-change", a.domain)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+a.token)
	req.Header.Add("Content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		r, _ := ioutil.ReadAll(res.Body)
		return nil, fmt.Errorf("[CreatePasswordChangeTicket] invalid status code: %d, body=%s", res.StatusCode, string(r))
	}

	r, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var resp CreatePasswordChangeTicketResponse
	err = json.Unmarshal(r, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

func (a *Service) PatchUserAppMetadata(userId string, appMetadata Metadata) error {
	if err := a.fillToken(); err != nil {
		return err
	}

	js, err := json.Marshal(appMetadata)
	if err != nil {
		return err
	}

	js = []byte(fmt.Sprintf(`{"app_metadata": %s}`, string(js)))

	url := fmt.Sprintf("%s/api/v2/users/%s", a.domain, userId)
	req, err := http.NewRequest("PATCH", url, bytes.NewReader(js))
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", "Bearer "+a.token)
	req.Header.Add("Content-type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		r, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("[PatchUserAppMetadata] invalid status code: %d, body=%s", res.StatusCode, string(r))
	}
	return nil
}

func (a *Service) SearchUsersByWorkspace(wsID string) ([]User, error) {
	if err := a.fillToken(); err != nil {
		return nil, err
	}
	url, err := url2.Parse(fmt.Sprintf("%s/api/v2/users", a.domain))
	if err != nil {
		return nil, err
	}

	queryString := url.Query()
	queryString.Set("search_engine", "v3")
	queryString.Set("q", "_exists_:app_metadata.workspaceAccess."+wsID)
	url.RawQuery = queryString.Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+a.token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		r, _ := ioutil.ReadAll(res.Body)
		return nil, fmt.Errorf("[SearchUsersByWorkspace] invalid status code: %d, body=%s", res.StatusCode, string(r))
	}

	r, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var resp []User
	err = json.Unmarshal(r, &resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (a *Service) SearchUsers(wsID string, email *string, emailVerified *bool, role *api.Role) ([]User, error) {
	if err := a.fillToken(); err != nil {
		return nil, err
	}
	url, err := url2.Parse(fmt.Sprintf("%s/api/v2/users", a.domain))
	if err != nil {
		return nil, err
	}

	queryString := url.Query()
	queryString.Set("search_engine", "v3")
	query := "_exists_:app_metadata.workspaceAccess." + wsID
	if emailVerified != nil {
		query = query + " AND email_verified:" + strconv.FormatBool(*emailVerified)
	}
	if email != nil {
		query = query + " AND email:" + *email
	}
	queryString.Set("q", query)
	url.RawQuery = queryString.Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+a.token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		r, _ := ioutil.ReadAll(res.Body)
		return nil, fmt.Errorf("[SearchUsers] invalid status code: %d, body=%s", res.StatusCode, string(r))
	}

	r, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var users []User
	err = json.Unmarshal(r, &users)
	if err != nil {
		return nil, err
	}

	var resp []User
	if role != nil {
		for _, user := range users {
			if func() bool {
				for _, r := range user.AppMetadata.WorkspaceAccess {
					if r == *role {
						return true
					}
				}
				return false
			}() {
				resp = append(resp, user)
			}
		}
	} else {
		resp = users
	}
	return resp, nil
}
