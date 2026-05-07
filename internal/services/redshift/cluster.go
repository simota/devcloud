package redshift

import (
	"encoding/xml"
	"net/http"
	"sort"
	"strconv"
	"time"
)

type ClusterSnapshot struct {
	ClusterIdentifier string          `json:"clusterIdentifier"`
	ClusterStatus     string          `json:"clusterStatus"`
	DatabaseName      string          `json:"databaseName"`
	Endpoint          ClusterEndpoint `json:"endpoint"`
	NodeType          string          `json:"nodeType"`
	NumberOfNodes     int             `json:"numberOfNodes"`
	MasterUsername    string          `json:"masterUsername"`
	Tags              []Tag           `json:"tags,omitempty"`
}

type ClusterSnapshotMetadata struct {
	SnapshotIdentifier string `json:"snapshotIdentifier"`
	ClusterIdentifier  string `json:"clusterIdentifier"`
	SnapshotCreateTime string `json:"snapshotCreateTime"`
	Status             string `json:"status"`
	Port               int    `json:"port"`
	AvailabilityZone   string `json:"availabilityZone"`
	ClusterCreateTime  string `json:"clusterCreateTime"`
	MasterUsername     string `json:"masterUsername"`
	ClusterVersion     string `json:"clusterVersion"`
	EngineFullVersion  string `json:"engineFullVersion"`
	NodeType           string `json:"nodeType"`
	NumberOfNodes      int    `json:"numberOfNodes"`
	DBName             string `json:"dbName"`
	Encrypted          bool   `json:"encrypted"`
}

type ClusterEndpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type Tag struct {
	Key   string `json:"key" xml:"Key"`
	Value string `json:"value" xml:"Value"`
}

type serverlessListRequest struct {
	MaxResults int    `json:"maxResults,omitempty"`
	NextToken  string `json:"nextToken,omitempty"`
}

type serverlessNamespaceRequest struct {
	NamespaceName string `json:"namespaceName"`
}

type serverlessWorkgroupRequest struct {
	WorkgroupName string `json:"workgroupName"`
}

type serverlessNamespace struct {
	NamespaceName string `json:"namespaceName"`
	DBName        string `json:"dbName"`
	Status        string `json:"status"`
}

type serverlessWorkgroup struct {
	WorkgroupName string          `json:"workgroupName"`
	NamespaceName string          `json:"namespaceName"`
	Status        string          `json:"status"`
	Endpoint      ClusterEndpoint `json:"endpoint"`
}

func (s *Server) clusterSnapshot() ClusterSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cluster, ok := s.clusters[defaultClusterIdentifier(s.config)]; ok {
		return cluster
	}
	return clusterSnapshotFromConfig(s.config)
}

func (s *Server) clusterXML() clusterXML {
	return clusterXMLFromSnapshot(s.clusterSnapshot())
}

func (s *Server) serverlessNamespace() serverlessNamespace {
	cluster := s.clusterSnapshot()
	database := defaultString(cluster.DatabaseName, defaultString(s.config.Database, "dev"))
	return serverlessNamespace{
		NamespaceName: database,
		DBName:        database,
		Status:        "AVAILABLE",
	}
}

func (s *Server) serverlessWorkgroup() serverlessWorkgroup {
	cluster := s.clusterSnapshot()
	return serverlessWorkgroup{
		WorkgroupName: cluster.ClusterIdentifier,
		NamespaceName: defaultString(cluster.DatabaseName, defaultString(s.config.Database, "dev")),
		Status:        "AVAILABLE",
		Endpoint:      cluster.Endpoint,
	}
}

func (s *Server) clusterSnapshotFromForm(r *http.Request) ClusterSnapshot {
	cluster := clusterSnapshotFromConfig(s.config)
	cluster.ClusterIdentifier = defaultString(r.Form.Get("ClusterIdentifier"), cluster.ClusterIdentifier)
	cluster.DatabaseName = defaultString(r.Form.Get("DBName"), cluster.DatabaseName)
	cluster.NodeType = defaultString(r.Form.Get("NodeType"), cluster.NodeType)
	cluster.MasterUsername = defaultString(r.Form.Get("MasterUsername"), cluster.MasterUsername)
	if nodes, err := strconv.Atoi(r.Form.Get("NumberOfNodes")); err == nil && nodes > 0 {
		cluster.NumberOfNodes = nodes
	}
	cluster.Tags = parseTagMembers(r.Form)
	return cluster
}

func (s *Server) clusterSnapshotsLocked() []ClusterSnapshot {
	ids := make([]string, 0, len(s.clusters))
	for id := range s.clusters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	clusters := make([]ClusterSnapshot, 0, len(ids))
	for _, id := range ids {
		clusters = append(clusters, s.clusters[id])
	}
	return clusters
}

func (s *Server) clusterXMLsLocked() []clusterXML {
	clusters := s.clusterSnapshotsLocked()
	result := make([]clusterXML, 0, len(clusters))
	for _, cluster := range clusters {
		result = append(result, clusterXMLFromSnapshot(cluster))
	}
	return result
}

func (s *Server) taggedResourcesLocked(resourceName string) []taggedResourceXML {
	clusters := s.clusterSnapshotsLocked()
	result := make([]taggedResourceXML, 0)
	for _, cluster := range clusters {
		arn := s.clusterARN(cluster.ClusterIdentifier)
		if resourceName != "" && resourceName != arn {
			continue
		}
		for _, tag := range cluster.Tags {
			result = append(result, taggedResourceXML{
				ResourceName: arn,
				ResourceType: "cluster",
				Tag:          tag,
			})
		}
	}
	return result
}

func (s *Server) clusterByResourceNameLocked(resourceName string) (ClusterSnapshot, string, bool) {
	for id, cluster := range s.clusters {
		if resourceName == s.clusterARN(id) {
			return cluster, id, true
		}
	}
	return ClusterSnapshot{}, "", false
}

func (s *Server) clusterARN(identifier string) string {
	return "arn:aws:redshift:" + defaultString(s.config.Region, "us-east-1") + ":" + defaultString(s.config.AccountID, "000000000000") + ":cluster:" + identifier
}

func (s *Server) writeClusterActionXML(w http.ResponseWriter, responseName string, resultName string, cluster ClusterSnapshot) {
	response := clusterActionResponse{
		XMLName:    xml.Name{Local: responseName},
		Xmlns:      "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:  "devcloud-redshift",
		ResultName: xml.Name{Local: resultName},
		Cluster:    clusterXMLFromSnapshot(cluster),
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *Server) writeClusterSnapshotActionXML(w http.ResponseWriter, responseName string, resultName string, snapshot ClusterSnapshotMetadata) {
	response := clusterSnapshotActionResponse{
		XMLName:    xml.Name{Local: responseName},
		Xmlns:      "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID:  "devcloud-redshift",
		ResultName: xml.Name{Local: resultName},
		Snapshot:   clusterSnapshotXMLFromMetadata(snapshot),
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func writeEmptyQueryXML(w http.ResponseWriter, responseName string) {
	response := emptyQueryResponse{
		XMLName:   xml.Name{Local: responseName},
		Xmlns:     "http://redshift.amazonaws.com/doc/2012-12-01/",
		RequestID: "devcloud-redshift",
	}
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func clusterSnapshotFromConfig(cfg Config) ClusterSnapshot {
	return ClusterSnapshot{
		ClusterIdentifier: defaultClusterIdentifier(cfg),
		ClusterStatus:     "available",
		DatabaseName:      defaultString(cfg.Database, "dev"),
		Endpoint: ClusterEndpoint{
			Address: hostFromAddr(cfg.SQLAddr),
			Port:    portFromAddr(cfg.SQLAddr, 5439),
		},
		NodeType:       defaultString(cfg.NodeType, "dc2.large"),
		NumberOfNodes:  positiveOrDefault(cfg.NumberOfNodes, 1),
		MasterUsername: defaultString(cfg.User, "dev"),
	}
}

func clusterXMLFromSnapshot(cluster ClusterSnapshot) clusterXML {
	return clusterXML{
		ClusterIdentifier: cluster.ClusterIdentifier,
		ClusterStatus:     cluster.ClusterStatus,
		DBName:            cluster.DatabaseName,
		Endpoint: endpointXML{
			Address: cluster.Endpoint.Address,
			Port:    cluster.Endpoint.Port,
		},
		NodeType:       cluster.NodeType,
		NumberOfNodes:  cluster.NumberOfNodes,
		MasterUsername: cluster.MasterUsername,
	}
}

func defaultClusterIdentifier(cfg Config) string {
	return defaultString(cfg.ClusterIdentifier, "devcloud")
}

func (s *Server) clusterSnapshotXMLsLocked(clusterIdentifier string, snapshotIdentifier string) []clusterSnapshotXML {
	ids := make([]string, 0, len(s.snapshots))
	for id, snapshot := range s.snapshots {
		if snapshotIdentifier != "" && id != snapshotIdentifier {
			continue
		}
		if clusterIdentifier != "" && snapshot.ClusterIdentifier != clusterIdentifier {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]clusterSnapshotXML, 0, len(ids))
	for _, id := range ids {
		result = append(result, clusterSnapshotXMLFromMetadata(s.snapshots[id]))
	}
	return result
}

func clusterSnapshotMetadataFromCluster(identifier string, cluster ClusterSnapshot) ClusterSnapshotMetadata {
	return ClusterSnapshotMetadata{
		SnapshotIdentifier: identifier,
		ClusterIdentifier:  cluster.ClusterIdentifier,
		SnapshotCreateTime: time.Now().UTC().Format(time.RFC3339),
		Status:             "available",
		Port:               cluster.Endpoint.Port,
		AvailabilityZone:   "devcloud-local",
		ClusterCreateTime:  time.Now().UTC().Format(time.RFC3339),
		MasterUsername:     cluster.MasterUsername,
		ClusterVersion:     "1.0",
		EngineFullVersion:  "devcloud-redshift-1.0",
		NodeType:           cluster.NodeType,
		NumberOfNodes:      cluster.NumberOfNodes,
		DBName:             cluster.DatabaseName,
		Encrypted:          false,
	}
}

func clusterSnapshotFromSnapshotMetadata(identifier string, snapshot ClusterSnapshotMetadata, cfg Config) ClusterSnapshot {
	return ClusterSnapshot{
		ClusterIdentifier: identifier,
		ClusterStatus:     "available",
		DatabaseName:      defaultString(snapshot.DBName, defaultString(cfg.Database, "dev")),
		Endpoint: ClusterEndpoint{
			Address: hostFromAddr(cfg.SQLAddr),
			Port:    positiveOrDefault(snapshot.Port, portFromAddr(cfg.SQLAddr, 5439)),
		},
		NodeType:       defaultString(snapshot.NodeType, defaultString(cfg.NodeType, "dc2.large")),
		NumberOfNodes:  positiveOrDefault(snapshot.NumberOfNodes, positiveOrDefault(cfg.NumberOfNodes, 1)),
		MasterUsername: defaultString(snapshot.MasterUsername, defaultString(cfg.User, "dev")),
	}
}
