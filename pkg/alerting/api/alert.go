package api

<<<<<<< Updated upstream
type SlackResponse struct {
=======
type SlackRequest struct {
>>>>>>> Stashed changes
	ChannelName string `json:"channel_name"`
	Text        string `json:"text"`
}

type SlackInputs struct {
	SlackUrl    string `json:"slack_url"`
	ChannelName string `json:"channel_name"`
	RuleId      int    `json:"rule_id"`
}
<<<<<<< Updated upstream
=======

type JiraInputs struct {
	AtlassianDomain   string `json:"atlassian_domain"`
	AtlassianApiToken string `json:"atlassian_api_token"`
	Email             string `json:"email"`
	IssueTypeId       string `json:"issue_type_id"`
	ProjectId         string `json:"project_id"`
}
>>>>>>> Stashed changes
