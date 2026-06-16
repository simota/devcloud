export const recentDynamoDBOperationsStorageKey = 'devcloud.dynamodb.recentOperations.v1'
export const maxRecentDynamoDBOperations = 10

export const defaultCreateTableJSON = `{
  "TableName": "Demo",
  "AttributeDefinitions": [
    { "AttributeName": "pk", "AttributeType": "S" }
  ],
  "KeySchema": [
    { "AttributeName": "pk", "KeyType": "HASH" }
  ],
  "BillingMode": "PAY_PER_REQUEST"
}`

export const defaultPutItemJSON = `{
  "Item": {
    "pk": { "S": "user#1" },
    "name": { "S": "Ada" }
  }
}`

export const defaultUpdateItemJSON = `{
  "Key": {
    "pk": { "S": "user#1" }
  },
  "UpdateExpression": "SET #name = :name",
  "ExpressionAttributeNames": {
    "#name": "name"
  },
  "ExpressionAttributeValues": {
    ":name": { "S": "Grace" }
  }
}`

export const defaultDeleteItemJSON = `{
  "Key": {
    "pk": { "S": "user#1" }
  }
}`

export const defaultTTLJSON = `{
  "TimeToLiveSpecification": {
    "Enabled": true,
    "AttributeName": "expiresAt"
  }
}`

export const defaultExpressionAttributeValuesJSON = `{
  ":pk": { "S": "user#1" }
}`
