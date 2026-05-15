package applicationautoscaling

import "strings"

func scalableTargetKey(namespace, resourceID, dimension string) string {
	return strings.Join([]string{namespace, resourceID, dimension}, "|")
}

func scalingPolicyKey(namespace, resourceID, dimension, policyName string) string {
	return strings.Join([]string{namespace, resourceID, dimension, policyName}, "|")
}

func scheduledActionKey(namespace, resourceID, dimension, name string) string {
	return strings.Join([]string{namespace, resourceID, dimension, name}, "|")
}
