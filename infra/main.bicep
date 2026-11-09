// Service Bus resources for the event orchestrator.
// Deploy: az deployment group create -g <rg> -f infra/main.bicep -p namespaceName=<unique-name> principalId=<object-id>

@description('Globally unique Service Bus namespace name.')
param namespaceName string

param location string = resourceGroup().location

@description('Object ID of the identity the orchestrator runs as (managed identity or developer).')
param principalId string

@description('Service principal for a managed identity, User for a developer.')
@allowed(['ServicePrincipal', 'User'])
param principalType string = 'ServicePrincipal'

var inboundQueues = ['inbound-1', 'inbound-2', 'inbound-3', 'inbound-4', 'inbound-5']

// Standard tier is the minimum: Basic has no transactions or duplicate detection.
resource sb 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' = {
  name: namespaceName
  location: location
  sku: { name: 'Standard', tier: 'Standard' }
  properties: {
    disableLocalAuth: true   // Entra ID only, no SAS keys
    minimumTlsVersion: '1.2'
  }
}

resource inbound 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = [for q in inboundQueues: {
  parent: sb
  name: q
  properties: {
    lockDuration: 'PT1M'
    maxDeliveryCount: 10                  // poison messages go to the DLQ instead of looping forever
    deadLetteringOnMessageExpiration: true
    requiresDuplicateDetection: false     // the gateway gives returned events a new MessageId anyway
  }
}]

resource outbound 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: sb
  name: 'outbound'
  properties: {
    lockDuration: 'PT1M'
    maxDeliveryCount: 10
    requiresDuplicateDetection: true      // MessageId = EventId, so a retried publish does not create duplicates
    duplicateDetectionHistoryTimeWindow: 'PT1H'
    deadLetteringOnMessageExpiration: true
  }
}

resource staging 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: sb
  name: 'orchestrator-staging'
  properties: {
    lockDuration: 'PT1M'
    maxDeliveryCount: 10
  }
}

// Azure Service Bus Data Owner: send, receive and complete on every queue in the namespace.
// For least privilege, split it into Data Receiver on inbound/staging and Data Sender on outbound/staging/inbound.
resource dataOwner 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(sb.id, principalId, 'sb-data-owner')
  scope: sb
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '090c5cfd-751d-490a-894a-3ce6f1109419')
    principalId: principalId
    principalType: principalType
  }
}

output fullyQualifiedNamespace string = '${sb.name}.servicebus.windows.net'
