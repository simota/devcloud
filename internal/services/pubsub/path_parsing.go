package pubsub

import (
	"net/url"
	"strings"
)

func isTopicsCollectionPath(path string) bool {
	parts := pathParts(path)
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "topics" && parts[2] != ""
}

func isTopicPublishPath(path string) bool {
	_, _, ok := topicPublishParts(path)
	return ok
}

func isTopicIAMPath(path string) bool {
	_, _, action, ok := topicActionParts(path)
	return ok && isIAMAction(action)
}

func isTopicPath(path string) bool {
	_, _, ok := topicNameParts(path)
	return ok
}

func isTopicSubscriptionsPath(path string) bool {
	_, _, ok := topicSubscriptionsParts(path)
	return ok
}

func isTopicSnapshotsPath(path string) bool {
	_, _, ok := topicSnapshotsParts(path)
	return ok
}

func isSubscriptionsCollectionPath(path string) bool {
	parts := pathParts(path)
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "subscriptions" && parts[2] != ""
}

func isSnapshotsCollectionPath(path string) bool {
	parts := pathParts(path)
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "snapshots" && parts[2] != ""
}

func isSchemasCollectionPath(path string) bool {
	parts := pathParts(path)
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "projects" && parts[3] == "schemas" && parts[2] != ""
}

func isSchemasValidateMessagePath(path string) bool {
	_, ok := schemasValidateMessageParts(path)
	return ok
}

func isSubscriptionPullPath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "pull")
	return ok
}

func isSubscriptionAcknowledgePath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "acknowledge")
	return ok
}

func isSubscriptionModifyAckDeadlinePath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "modifyAckDeadline")
	return ok
}

func isSubscriptionModifyPushConfigPath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "modifyPushConfig")
	return ok
}

func isSubscriptionDetachPath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "detach")
	return ok
}

func isSubscriptionIAMPath(path string) bool {
	_, _, action, ok := subscriptionAnyActionParts(path)
	return ok && isIAMAction(action)
}

func isSubscriptionSeekPath(path string) bool {
	_, _, ok := subscriptionActionParts(path, "seek")
	return ok
}

func isSubscriptionPath(path string) bool {
	_, _, ok := subscriptionNameParts(path)
	return ok
}

func isSnapshotPath(path string) bool {
	_, _, ok := snapshotNameParts(path)
	return ok
}

func isSchemaPath(path string) bool {
	_, _, ok := schemaNameParts(path)
	return ok
}

func pathParts(path string) []string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		unescaped, err := url.PathUnescape(part)
		if err != nil {
			parts[i] = "\x00"
			continue
		}
		parts[i] = strings.TrimSpace(unescaped)
	}
	return parts
}

func topicNameParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "topics" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func topicPublishParts(path string) (string, string, bool) {
	project, topicID, action, ok := topicActionParts(path)
	if !ok || action != "publish" {
		return "", "", false
	}
	return project, topicID, true
}

func topicActionParts(path string) (string, string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "topics" || parts[2] == "" {
		return "", "", "", false
	}
	topicID, action, ok := strings.Cut(parts[4], ":")
	if !ok || topicID == "" || action == "" {
		return "", "", "", false
	}
	return parts[2], topicID, action, true
}

func topicSubscriptionsParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 6 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "topics" || parts[5] != "subscriptions" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func topicSnapshotsParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 6 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "topics" || parts[5] != "snapshots" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func subscriptionActionParts(path string, wantAction string) (string, string, bool) {
	project, subscriptionID, action, ok := subscriptionAnyActionParts(path)
	if !ok || action != wantAction {
		return "", "", false
	}
	return project, subscriptionID, true
}

func subscriptionAnyActionParts(path string) (string, string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "subscriptions" || parts[2] == "" {
		return "", "", "", false
	}
	subscriptionID, action, ok := strings.Cut(parts[4], ":")
	if !ok || subscriptionID == "" || action == "" {
		return "", "", "", false
	}
	return parts[2], subscriptionID, action, true
}

func subscriptionNameParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "subscriptions" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func snapshotNameParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "snapshots" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func schemaNameParts(path string) (string, string, bool) {
	parts := pathParts(path)
	if len(parts) != 5 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "schemas" || parts[2] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[2], parts[4], true
}

func schemasValidateMessageParts(path string) (string, bool) {
	parts := pathParts(path)
	if len(parts) != 4 || parts[0] != "v1" || parts[1] != "projects" || parts[2] == "" || parts[3] != "schemas:validateMessage" {
		return "", false
	}
	return parts[2], true
}
