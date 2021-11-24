package describer

import (
	"strings"
)

type Resource struct {
	// ARN uniquely identifies an AWS resource across regions, accounts and types.
	ARN string
	// ID doesn't uniquely identifies a resource. It will be used to create a
	// unique identifier by concating REGION|ACCOUNT|TYPE|ID
	ID          string
	Description interface{}

	Account string
	Region  string
	Type    string
}

func (r Resource) UniqueID() string {
	if r.ARN != "" {
		return r.ARN
	}

	return CompositeID(r.Region, r.Account, r.Type, r.ID)
}

func CompositeID(list ...string) string {
	normList := make([]string, 0, len(list))
	for _, v := range list {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}

		normList = append(normList, v)

	}
	return strings.Join(normList, "|")
}
